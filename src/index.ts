interface McpToolDefinition {
  name: string;
  description: string;
  /** Human-facing one-liner (fleet #1967). Optional; consumers fall back to
   *  description. Kept in step with shared/src/types.ts — scripts/lib/
   *  check-inlined-types.mjs reports drift at publish time. */
  summary?: string;
  inputSchema: {
    type: 'object';
    properties: Record<string, unknown>;
    required?: string[];
    anyOf?: Array<{ required: string[] }>;
    oneOf?: Array<{ required: string[] }>;
    allOf?: Array<{ required: string[] }>;
  };
  outputSchema?: Record<string, unknown>;
}

interface McpToolExport {
  tools: McpToolDefinition[];
  callTool: (name: string, args: Record<string, unknown>) => Promise<unknown>;
  meter?: { credits: number };
  cost?: Record<string, unknown>;
  provider?: string;
}

/**
 * Overture Maps places — business and point-of-interest locations (name,
 * category, confidence, address, websites, phones, socials, brand) for the
 * United States and the United Kingdom, from the Overture Maps Foundation
 * places theme (CDLA-Permissive-2.0 / Apache-2.0 / CC0-1.0 by source).
 *
 * Three questions it answers:
 *   places_search    — "find <name or category> near here / in this city"
 *   places_by_domain — "where are all the locations of the company at acme.com"
 *   places_in_area   — "how many <category> are in this area, and which"
 *
 * HOW IT READS (fleet #2871, docs/overture-places-design.md). Overture ships the
 * theme only as monthly GeoParquet releases — there is no per-query API — so a
 * GitHub Actions job (overture-places-ingest.yml) turns each release into
 * range-readable gzip blocks in the gateway's dataset bucket, injected here as
 * `_r2`. A call reads `current.json` (which release is live), the release
 * manifest, one or more small index files, and then exactly the blocks it
 * needs by byte range. Nothing is baked into this module.
 *
 * KEY FUNCTIONS MIRROR scripts/overture-places-build.py EXACTLY: the quadkey
 * math, baseDomain(), localityKey() and the sha256 shard prefixes. Change one
 * side and change the other; the key-vector tests pin vectors
 * the Python build produced (src/index.test.ts).
 *
 * FAILURE SHAPES. A missing store object, an unreadable block or a corrupt
 * index THROWS — it is our outage and must read as one. found:false is only
 * for real answers: outside coverage, an unknown locality, a platform domain,
 * or a domain no place lists.
 */


// ── store access ─────────────────────────────────────────────────────────────

interface R2Range { offset: number; length: number }
interface R2ObjectLike { arrayBuffer(): Promise<ArrayBuffer> }
export interface R2Like {
  get(key: string, opts?: { range?: R2Range }): Promise<R2ObjectLike | null>;
}

const ROOT = 'overture-places';
export const DEFAULT_MIN_CONFIDENCE = 0.7;
const MAX_LEAVES = 64;
const MAX_GROUPS = 200;
const READ_CONCURRENCY = 8;
const CACHE_TTL_MS = 10 * 60 * 1000;
const Z_GROUP = 6;

class StoreError extends Error {
  constructor(msg: string) {
    super(`overture-places: ${msg}`);
    this.name = 'StoreError';
  }
}

function storeFrom(args: Record<string, unknown>): R2Like {
  const r2 = args._r2 as R2Like | undefined;
  if (!r2 || typeof r2.get !== 'function') {
    throw new StoreError(
      'the places data store is not available to this process. This pack answers only through ' +
        'the Pipeworx gateway (https://gateway.pipeworx.io/mcp); a standalone install has no ' +
        'Overture release to read.',
    );
  }
  return r2;
}

// A small per-isolate cache of index objects (pointer, manifest, z6 tile
// indexes). Bounded and keyed by release, so a release swap is picked up
// within CACHE_TTL_MS and memory cannot grow with traffic.
const cache = new Map<string, { at: number; v: unknown }>();
function cacheGet<T>(k: string): T | undefined {
  const hit = cache.get(k);
  if (hit && Date.now() - hit.at < CACHE_TTL_MS) return hit.v as T;
  if (hit) cache.delete(k);
  return undefined;
}
function cachePut(k: string, v: unknown): void {
  if (cache.size >= 256) cache.delete(cache.keys().next().value as string);
  cache.set(k, { at: Date.now(), v });
}

async function readObject(r2: R2Like, key: string, range?: R2Range): Promise<ArrayBuffer> {
  let obj: R2ObjectLike | null;
  try {
    obj = await r2.get(key, range ? { range } : undefined);
  } catch (e) {
    throw new StoreError(`store read failed for ${key}: ${(e as Error).message}`);
  }
  if (!obj) throw new StoreError(`store object missing: ${key}`);
  return obj.arrayBuffer();
}

async function readJson<T>(r2: R2Like, key: string): Promise<T> {
  const cached = cacheGet<T>(key);
  if (cached !== undefined) return cached;
  const buf = await readObject(r2, key);
  let v: T;
  try {
    v = JSON.parse(new TextDecoder().decode(buf)) as T;
  } catch (e) {
    throw new StoreError(`store object ${key} is not valid JSON: ${(e as Error).message}`);
  }
  cachePut(key, v);
  return v;
}

async function gunzipLines(buf: ArrayBuffer, what: string): Promise<string[]> {
  try {
    const stream = new Response(buf).body!.pipeThrough(new DecompressionStream('gzip'));
    const text = await new Response(stream).text();
    return text.split('\n').filter((l) => l.length > 0);
  } catch (e) {
    throw new StoreError(`could not decompress ${what}: ${(e as Error).message}`);
  }
}

function parseLine<T>(line: string, what: string): T {
  try {
    return JSON.parse(line) as T;
  } catch (e) {
    throw new StoreError(`corrupt row in ${what}: ${(e as Error).message}`);
  }
}

interface Pointer { release: string; prefix: string; data_as_of: string; countries: string[]; rows: number }
interface Manifest {
  release: string;
  data_as_of: string;
  countries: string[];
  rows: number;
  rows_by_country: Record<string, number>;
  default_min_confidence: number;
  groups: string[];
  domain_shards: number;
  domain_cap: number;
  domain_excluded: string[];
}
interface Release { r2: R2Like; prefix: string; manifest: Manifest; groups: Set<string> }

async function openRelease(args: Record<string, unknown>): Promise<Release> {
  const r2 = storeFrom(args);
  const ptr = await readJson<Pointer>(r2, `${ROOT}/current.json`);
  if (!ptr?.prefix || !ptr.release) throw new StoreError('current.json carries no release pointer');
  const manifest = await readJson<Manifest>(r2, `${ptr.prefix}/manifest.json`);
  if (!Array.isArray(manifest.groups) || manifest.groups.length === 0 || !manifest.rows) {
    throw new StoreError(`release ${ptr.release} manifest lists no data — refusing to answer from an empty release`);
  }
  const gk = `groups:${ptr.prefix}`;
  let groups = cacheGet<Set<string>>(gk);
  if (!groups) {
    groups = new Set(manifest.groups);
    cachePut(gk, groups);
  }
  return { r2, prefix: ptr.prefix, manifest, groups };
}

function binKey(rel: Release, bin: number): string {
  return `${rel.prefix}/bin/${String(bin).padStart(3, '0')}.bin`;
}

/** 20-byte big-endian entries: bin, offset_hi, offset_lo, length, rows. */
async function readIdxEntry(rel: Release, file: string, shard: number): Promise<{ bin: number; offset: number; length: number; rows: number }> {
  const buf = await readObject(rel.r2, `${rel.prefix}/${file}`, { offset: shard * 20, length: 20 });
  if (buf.byteLength !== 20) throw new StoreError(`${file} entry ${shard} is ${buf.byteLength} bytes, expected 20`);
  const dv = new DataView(buf);
  return {
    bin: dv.getUint32(0),
    offset: dv.getUint32(4) * 2 ** 32 + dv.getUint32(8),
    length: dv.getUint32(12),
    rows: dv.getUint32(16),
  };
}

async function mapLimit<T, R>(items: T[], n: number, fn: (t: T) => Promise<R>): Promise<R[]> {
  const out: R[] = new Array(items.length);
  let i = 0;
  const workers = Array.from({ length: Math.min(n, items.length) }, async () => {
    while (i < items.length) {
      const k = i++;
      out[k] = await fn(items[k]);
    }
  });
  await Promise.all(workers);
  return out;
}

// ── keys shared with the Python build ────────────────────────────────────────

export async function sha256Hex(s: string): Promise<string> {
  const d = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
  return Array.from(new Uint8Array(d), (b) => b.toString(16).padStart(2, '0')).join('');
}

const SLD_RE = /(\.(co|com|org|net|ac|gov|ltd|plc|me|nhs|sch|police|mod|edu)\.[a-z]{2}|\.(uk|us|gb|eu)\.(com|net)|\.[a-z]{2}\.us)$/;

/** Mirrors HOST_SQL + the base-domain CASE in overture-places-build.py. */
export function baseDomain(input: string): string | null {
  let h = input.trim().toLowerCase();
  h = h.replace(/^[a-z][a-z0-9+.-]*:\/\//, '');
  h = h.replace(/[/?#:].*$/, '');
  h = h.replace(/^www[0-9]*\./, '');
  h = h.replace(/\.$/, '');
  if (!h.includes('.') || /^[0-9.]+$/.test(h)) return null;
  const parts = h.split('.');
  return SLD_RE.test(h) ? parts.slice(-3).join('.') : parts.slice(-2).join('.');
}

/** Mirrors LOCALITY_KEY_SQL: strip accents, lowercase, collapse whitespace. */
export function localityKey(s: string): string {
  return s.normalize('NFD').replace(/\p{M}/gu, '').toLowerCase().trim().replace(/\s+/g, ' ');
}

function tileX(lon: number, z: number): number {
  const n = 2 ** z;
  return Math.min(n - 1, Math.max(0, Math.floor(((lon + 180) / 360) * n)));
}
function tileY(lat: number, z: number): number {
  const n = 2 ** z;
  const l = (Math.max(-85.0511, Math.min(85.0511, lat)) * Math.PI) / 180;
  return Math.min(n - 1, Math.max(0, Math.floor(((1 - Math.log(Math.tan(l) + 1 / Math.cos(l)) / Math.PI) / 2) * n)));
}
export function quadkeyForPoint(lon: number, lat: number, z: number): string {
  return quadkeyOf(tileX(lon, z), tileY(lat, z), z);
}
export function quadkeyOf(x: number, y: number, z: number): string {
  let q = '';
  for (let i = z; i > 0; i--) q += String(((x >> (i - 1)) & 1) + 2 * ((y >> (i - 1)) & 1));
  return q;
}
export function quadkeyBounds(qk: string): Bbox {
  let x = 0;
  let y = 0;
  const z = qk.length;
  for (let i = 0; i < z; i++) {
    const d = Number(qk[i]);
    x = (x << 1) | (d & 1);
    y = (y << 1) | (d >> 1);
  }
  const n = 2 ** z;
  const lat = (yy: number) => (Math.atan(Math.sinh(Math.PI * (1 - (2 * yy) / n))) * 180) / Math.PI;
  return [(x / n) * 360 - 180, lat(y + 1), ((x + 1) / n) * 360 - 180, lat(y)];
}

// ── geometry and input resolution ────────────────────────────────────────────

type Bbox = [number, number, number, number]; // west, south, east, north

function intersects(a: Bbox, b: Bbox): boolean {
  return a[0] <= b[2] && a[2] >= b[0] && a[1] <= b[3] && a[3] >= b[1];
}
function contains(outer: Bbox, inner: Bbox): boolean {
  return inner[0] >= outer[0] && inner[2] <= outer[2] && inner[1] >= outer[1] && inner[3] <= outer[3];
}
function haversineM(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const r = (d: number) => (d * Math.PI) / 180;
  const a = Math.sin(r(lat2 - lat1) / 2) ** 2 + Math.cos(r(lat1)) * Math.cos(r(lat2)) * Math.sin(r(lon2 - lon1) / 2) ** 2;
  return 2 * 6371008.8 * Math.asin(Math.sqrt(a));
}

const COUNTRY_ALIASES: Record<string, string> = {
  US: 'US', USA: 'US', 'UNITED STATES': 'US', 'UNITED STATES OF AMERICA': 'US', AMERICA: 'US',
  GB: 'GB', UK: 'GB', 'UNITED KINGDOM': 'GB', 'GREAT BRITAIN': 'GB', BRITAIN: 'GB',
  ENGLAND: 'GB', SCOTLAND: 'GB', WALES: 'GB', 'NORTHERN IRELAND': 'GB',
};
function countryCode(v: unknown): string | null {
  if (typeof v !== 'string' || !v.trim()) return null;
  const k = v.trim().toUpperCase();
  return COUNTRY_ALIASES[k] ?? (k.length === 2 ? k : null);
}

const US_STATES: Record<string, string> = {
  alabama: 'AL', alaska: 'AK', arizona: 'AZ', arkansas: 'AR', california: 'CA', colorado: 'CO',
  connecticut: 'CT', delaware: 'DE', 'district of columbia': 'DC', florida: 'FL', georgia: 'GA',
  hawaii: 'HI', idaho: 'ID', illinois: 'IL', indiana: 'IN', iowa: 'IA', kansas: 'KS', kentucky: 'KY',
  louisiana: 'LA', maine: 'ME', maryland: 'MD', massachusetts: 'MA', michigan: 'MI', minnesota: 'MN',
  mississippi: 'MS', missouri: 'MO', montana: 'MT', nebraska: 'NE', nevada: 'NV', 'new hampshire': 'NH',
  'new jersey': 'NJ', 'new mexico': 'NM', 'new york': 'NY', 'north carolina': 'NC', 'north dakota': 'ND',
  ohio: 'OH', oklahoma: 'OK', oregon: 'OR', pennsylvania: 'PA', 'rhode island': 'RI',
  'south carolina': 'SC', 'south dakota': 'SD', tennessee: 'TN', texas: 'TX', utah: 'UT', vermont: 'VT',
  virginia: 'VA', washington: 'WA', 'west virginia': 'WV', wisconsin: 'WI', wyoming: 'WY',
};
function regionMatches(stored: string | null, wanted: string): boolean {
  const s = (stored ?? '').trim().toLowerCase();
  const w = wanted.trim().toLowerCase();
  if (!s) return false;
  return s === w || s === (US_STATES[w] ?? '').toLowerCase();
}

function num(v: unknown): number | null {
  const n = typeof v === 'string' ? Number(v) : v;
  return typeof n === 'number' && Number.isFinite(n) ? n : null;
}

class InputError extends Error {}

interface Locality { k: string; key: string; r: string | null; l: string; n: number; bbox: Bbox }
interface Area {
  bbox: Bbox;
  center: [number, number]; // lat, lon
  radius_m?: number;
  kind: 'near' | 'bbox' | 'city';
  resolved?: { locality: string; region: string | null; country: string; places_in_locality: number; region_variants_merged: number };
  alternatives?: Array<{ locality: string; region: string | null; country: string; places: number }>;
}
type NotFound = { found: false; reason: string; message: string; hint: string; [k: string]: unknown };

async function resolveArea(rel: Release, args: Record<string, unknown>, defaultRadius: number): Promise<Area | NotFound> {
  const near = (args.near && typeof args.near === 'object' ? args.near : {}) as Record<string, unknown>;
  const lat = num(near.lat ?? args.lat);
  const lon = num(near.lon ?? near.lng ?? args.lon ?? args.lng);
  if (lat !== null || lon !== null) {
    if (lat === null || lon === null || Math.abs(lat) > 90 || Math.abs(lon) > 180) {
      throw new InputError('near needs both lat (-90..90) and lon (-180..180) as numbers');
    }
    const radius = Math.min(50_000, Math.max(50, num(args.radius_m) ?? defaultRadius));
    const dLat = radius / 111_320;
    const dLon = radius / (111_320 * Math.max(0.01, Math.cos((lat * Math.PI) / 180)));
    return { kind: 'near', center: [lat, lon], radius_m: radius, bbox: [lon - dLon, lat - dLat, lon + dLon, lat + dLat] };
  }
  if (args.bbox !== undefined && args.bbox !== null && args.bbox !== '') {
    const raw = Array.isArray(args.bbox) ? args.bbox : String(args.bbox).split(',');
    const b = raw.map((v) => num(v));
    if (b.length !== 4 || b.some((v) => v === null)) {
      throw new InputError('bbox must be [west, south, east, north] in degrees (array or "w,s,e,n" string)');
    }
    const [w, s, e, n] = b as number[];
    if (w >= e || s >= n || Math.abs(s) > 90 || Math.abs(n) > 90 || Math.abs(w) > 180 || Math.abs(e) > 180) {
      throw new InputError('bbox must satisfy west < east and south < north (boxes crossing the antimeridian are not supported)');
    }
    return { kind: 'bbox', bbox: [w, s, e, n], center: [(s + n) / 2, (w + e) / 2] };
  }
  if (typeof args.city === 'string' && args.city.trim()) {
    const cc = countryCode(args.country);
    if (!cc) throw new InputError('city needs country (ISO code or name, e.g. "US" or "United Kingdom")');
    if (!rel.manifest.countries.includes(cc)) return outsideCoverage(rel, `country ${cc}`);
    const key = localityKey(args.city);
    const shard = parseInt((await sha256Hex(`${cc}|${key}`)).slice(0, 2), 16);
    const ent = await readIdxEntry(rel, 'localities.idx', shard);
    let rows: Locality[] = [];
    if (ent.length > 0) {
      const lines = await gunzipLines(await readObject(rel.r2, binKey(rel, ent.bin), { offset: ent.offset, length: ent.length }), 'locality index');
      rows = lines.map((l) => parseLine<Locality>(l, 'locality index')).filter((r) => r.k === cc && r.key === key);
    }
    const region = typeof args.region === 'string' && args.region.trim() ? args.region : null;
    if (region) rows = rows.filter((r) => regionMatches(r.r, region));
    if (rows.length === 0) {
      return {
        found: false,
        reason: 'unknown_locality',
        message: `No locality "${args.city}"${region ? ` in region ${region}` : ''} (${cc}) appears in the addresses of this release.`,
        hint: 'Check the spelling, drop or change region (US: state code like TX; UK regions are inconsistent upstream, so omit it), or pass near:{lat,lon} with radius_m or a bbox instead.',
      };
    }
    rows.sort((a, b) => b.n - a.n);
    const top = rows[0];
    // The same city under several region spellings (UK: "ENG", "Greater London",
    // "") is one place; a same-named city elsewhere (Springfield IL vs MO) is not.
    // Merge entries whose bbox centre falls inside the top entry's bbox.
    const merged = rows.filter((r) => {
      const c: [number, number] = [(r.bbox[1] + r.bbox[3]) / 2, (r.bbox[0] + r.bbox[2]) / 2];
      return c[0] >= top.bbox[1] && c[0] <= top.bbox[3] && c[1] >= top.bbox[0] && c[1] <= top.bbox[2];
    });
    const others = rows.filter((r) => !merged.includes(r));
    const bbox: Bbox = [top.bbox[0], top.bbox[1], top.bbox[2], top.bbox[3]];
    return {
      kind: 'city',
      bbox,
      center: [(bbox[1] + bbox[3]) / 2, (bbox[0] + bbox[2]) / 2],
      resolved: {
        locality: top.l,
        region: top.r || null,
        country: cc,
        places_in_locality: merged.reduce((s, r) => s + r.n, 0),
        region_variants_merged: merged.length,
      },
      ...(others.length
        ? { alternatives: others.slice(0, 10).map((r) => ({ locality: r.l, region: r.r || null, country: r.k, places: r.n })) }
        : {}),
    };
  }
  throw new InputError('give a location: near:{lat,lon} (with optional radius_m), bbox:[w,s,e,n], or city + country');
}

function outsideCoverage(rel: Release, what: string): NotFound {
  return {
    found: false,
    reason: 'outside_coverage',
    message: `${what} is outside the countries this pack currently carries.`,
    covered_countries: rel.manifest.countries,
    hint: `Covered: ${rel.manifest.countries.join(', ')}. For other countries try the foursquare pack (BYO key) or osm/nominatim-style packs via ask_pipeworx.`,
  };
}

// ── leaves ───────────────────────────────────────────────────────────────────

type LeafEntry = [number, number, number, number, number]; // bin, off, len, rows, rows>=0.7&open
interface Leaf { qk: string; bounds: Bbox; e: LeafEntry; dist: number }

async function leavesFor(rel: Release, area: Area): Promise<Leaf[] | NotFound> {
  const [w, s, e, n] = area.bbox;
  const x0 = tileX(w, Z_GROUP), x1 = tileX(e, Z_GROUP);
  const y0 = tileY(n, Z_GROUP), y1 = tileY(s, Z_GROUP);
  const groups: string[] = [];
  for (let x = x0; x <= x1; x++) {
    for (let y = y0; y <= y1; y++) {
      const g = quadkeyOf(x, y, Z_GROUP);
      if (rel.groups.has(g)) groups.push(g);
    }
  }
  if (groups.length === 0) return outsideCoverage(rel, 'This area');
  if (groups.length > MAX_GROUPS) {
    throw new InputError(`area spans ${groups.length} zoom-6 tiles; the limit is ${MAX_GROUPS} — narrow the bbox`);
  }
  const idx = await mapLimit(groups, READ_CONCURRENCY, (g) =>
    readJson<{ leaves: Record<string, LeafEntry> }>(rel.r2, `${rel.prefix}/tiles/${g}.json`),
  );
  const out: Leaf[] = [];
  for (const t of idx) {
    if (!t?.leaves) throw new StoreError('tile index carries no leaves');
    for (const [qk, ent] of Object.entries(t.leaves)) {
      const bounds = quadkeyBounds(qk);
      if (!intersects(bounds, area.bbox)) continue;
      const cLat = (bounds[1] + bounds[3]) / 2, cLon = (bounds[0] + bounds[2]) / 2;
      out.push({ qk, bounds, e: ent, dist: haversineM(area.center[0], area.center[1], cLat, cLon) });
    }
  }
  out.sort((a, b) => a.dist - b.dist);
  return out;
}

interface Row {
  i: string; n: string; c: string | null; t: string | null; h: string[] | null; f: number | null; s: string | null;
  x: number; y: number; a: string | null; l: string | null; r: string | null; p: string | null; k: string | null;
  w: string[] | null; ph: string[] | null; e: string[] | null; so: string[] | null; b: string | null; bw: string | null;
  src: string[] | null; lic: string[] | null; rid: string[] | null; d?: string; dt?: number;
}

async function readLeaf(rel: Release, leaf: Leaf): Promise<Row[]> {
  const [bin, off, len] = leaf.e;
  const lines = await gunzipLines(await readObject(rel.r2, binKey(rel, bin), { offset: off, length: len }), `leaf ${leaf.qk}`);
  return lines.map((l) => parseLine<Row>(l, `leaf ${leaf.qk}`));
}

// ── filters and shaping ──────────────────────────────────────────────────────

function normText(s: string): string {
  return s.normalize('NFKD').replace(/\p{M}/gu, '').toLowerCase().replace(/[^\p{L}\p{N}]+/gu, ' ').trim();
}
function normCategory(s: string): string {
  return s.trim().toLowerCase().replace(/[\s-]+/g, '_');
}
// Everyday words callers use for Overture's taxonomy terms. Small on purpose:
// the token-prefix rule below already maps "dentist" onto general_dentistry,
// cosmetic_dentistry and the rest; these are the ones no prefix can reach.
const CATEGORY_ALIASES: Record<string, string[]> = {
  dentist: ['dental_clinic'],
  dentists: ['dental_clinic'],
  doctor: ['doctors_office', 'family_practice', 'general_practitioner'],
  gp: ['general_practitioner', 'family_practice'],
  lawyer: ['attorney_or_law_firm'],
  lawyers: ['attorney_or_law_firm'],
  cafe: ['coffee_shop', 'cafe'],
  gym: ['gym', 'fitness_center'],
  pub: ['pub', 'bar'],
  'gas_station': ['gas_station', 'fuel_station'],
  petrol_station: ['gas_station', 'fuel_station'],
  supermarket: ['supermarket', 'grocery_store'],
  grocery: ['grocery_store', 'supermarket'],
  bank: ['bank_or_credit_union', 'bank'],
};

function categoryMatch(row: Row, cat: string): boolean {
  const fields = [row.c, row.t, ...(row.h ?? [])].filter((v): v is string => !!v);
  const exact = new Set([cat, cat.replace(/s$/, ''), cat.replace(/es$/, ''), ...(CATEGORY_ALIASES[cat] ?? [])]);
  if (fields.some((v) => exact.has(v))) return true;
  // A single word of 5+ letters also matches as the prefix of any word in a
  // category ("dentist" -> general_dentistry). Short words stay exact, so
  // "bar" never matches barber_shop.
  if (!cat.includes('_') && cat.length >= 5) {
    const stem = cat.replace(/s$/, '');
    return fields.some((v) => v.split('_').some((w) => w.startsWith(stem)));
  }
  return false;
}

interface Filters { minConf: number; includeClosed: boolean; category: string | null }
function filtersFrom(args: Record<string, unknown>): Filters {
  const mc = num(args.min_confidence);
  if (mc !== null && (mc < 0 || mc > 1)) throw new InputError('min_confidence must be between 0 and 1');
  return {
    minConf: mc ?? DEFAULT_MIN_CONFIDENCE,
    includeClosed: args.include_closed === true || args.include_closed === 'true',
    category: typeof args.category === 'string' && args.category.trim() ? normCategory(args.category) : null,
  };
}
function passes(row: Row, f: Filters): boolean {
  if ((row.f ?? 0) < f.minConf) return false;
  if (!f.includeClosed && row.s === 'permanently_closed') return false;
  if (f.category && !categoryMatch(row, f.category)) return false;
  return true;
}
function isDefaultFilter(f: Filters): boolean {
  return f.minConf === DEFAULT_MIN_CONFIDENCE && !f.includeClosed && !f.category;
}

function shape(row: Row, distance?: number) {
  return {
    id: row.i,
    name: row.n,
    category: row.c,
    taxonomy: { primary: row.t, hierarchy: row.h ?? [] },
    confidence: row.f,
    operating_status: row.s ?? null,
    lat: row.y,
    lon: row.x,
    ...(distance !== undefined ? { distance_m: Math.round(distance) } : {}),
    address: { freeform: row.a, locality: row.l, region: row.r, postcode: row.p, country: row.k },
    websites: (row.w ?? []).filter(Boolean),
    phones: row.ph ?? [],
    emails: row.e ?? [],
    socials: row.so ?? [],
    brand: row.b || row.bw ? { name: row.b, wikidata: row.bw } : null,
    sources: row.src ?? [],
    source_licenses: row.lic ?? [],
    source_record_ids: row.rid ?? [],
  };
}

const PER_SOURCE_CREDIT: Record<string, string> = {
  meta: 'Data from Meta. Available under CDLA Permissive 2.0.',
  Microsoft: 'Data from Microsoft. Available under CDLA Permissive 2.0.',
  PinMeTo: 'Data from PinMeTo. Available under CDLA Permissive 2.0.',
  Krick: 'Data from Krick. Available under CDLA Permissive 2.0.',
  RenderSEO: 'Data from RenderSEO. Available under CDLA Permissive 2.0.',
  DAC: 'Data from DAC. Available under CDLA Permissive 2.0.',
  BrightQuery: 'Data from BrightQuery. Available under CDLA Permissive 2.0.',
  Foursquare:
    'Data from Foursquare. Copyright 2024 Foursquare Labs, Inc. All rights reserved. Available under Apache 2.0. Foursquare data was transformed to the Overture schema.',
  AllThePlaces: 'Data from AllThePlaces. Available under CC0 1.0.',
};

/** The licence and attribution block every response leads with — the CDLA
 *  Permissive 2.0 / Apache 2.0 terms Overture publishes the theme under. */
function sourceBlock(rel: Release | null, rows: Row[] = []) {
  const datasets = new Set<string>();
  for (const r of rows) for (const d of r.src ?? []) datasets.add(d);
  const credits = [...datasets].filter((d) => PER_SOURCE_CREDIT[d]).sort().map((d) => PER_SOURCE_CREDIT[d]);
  const release = rel?.manifest.release ?? null;
  return {
    attribution: `© Overture Maps Foundation (https://overturemaps.org), places theme${release ? `, release ${release}` : ''}.${credits.length ? ' ' + credits.join(' ') : ''}`,
    source: {
      name: 'Overture Maps Foundation — places theme',
      url: 'https://overturemaps.org',
      release,
      license: 'CDLA-Permissive-2.0 (Foursquare-sourced records: Apache-2.0; AllThePlaces-sourced records: CC0-1.0)',
      license_url: 'https://cdla.dev/permissive-2-0/',
      attribution_terms: 'https://docs.overturemaps.org/attribution/',
      note: 'Redistribution is permitted with attribution. Each place lists its own contributing datasets and their licences in sources / source_licenses.',
    },
    data_as_of: rel?.manifest.data_as_of ?? null,
  };
}

function withSource(rel: Release, body: Record<string, unknown>, rows: Row[] = []) {
  return { ...sourceBlock(rel, rows), ...body };
}

function filterSummary(f: Filters) {
  return {
    min_confidence: f.minConf,
    include_closed: f.includeClosed,
    category: f.category,
    confidence_note:
      f.minConf === DEFAULT_MIN_CONFIDENCE
        ? `Rows below confidence ${DEFAULT_MIN_CONFIDENCE} are hidden by default (Overture: confidence is the primary junk filter). Pass min_confidence: 0 to see them.`
        : `Custom min_confidence ${f.minConf}; the default is ${DEFAULT_MIN_CONFIDENCE}.`,
  };
}

// ── tools ────────────────────────────────────────────────────────────────────

const LOCATION_PROPS = {
  near: {
    type: 'object',
    description: 'Centre point {lat, lon} in decimal degrees. Use with radius_m.',
    properties: { lat: { type: 'number' }, lon: { type: 'number' } },
  },
  radius_m: { type: 'number', description: 'Search radius in metres around near (50–50000).' },
  bbox: {
    type: 'array',
    items: { type: 'number' },
    description: 'Bounding box [west, south, east, north] in decimal degrees, as an alternative to near.',
  },
  city: { type: 'string', description: 'City / town / locality name, e.g. "Austin" or "London". Needs country.' },
  region: { type: 'string', description: 'Optional state or region to disambiguate city, e.g. "TX" or "Texas".' },
  country: { type: 'string', description: 'Country for city: "US" or "GB" (also accepts "UK", "United States", "United Kingdom").' },
} as const;
const FILTER_PROPS = {
  category: {
    type: 'string',
    description: 'Overture category, e.g. "coffee_shop", "dentist", "hotel", "restaurant", "pharmacy". Matches the basic category, the taxonomy primary or any level of its hierarchy.',
  },
  min_confidence: {
    type: 'number',
    description: `Hide places whose Overture existence confidence is below this (0–1). Default ${DEFAULT_MIN_CONFIDENCE}.`,
  },
  include_closed: { type: 'boolean', description: 'Include places Overture marks permanently_closed. Default false.' },
} as const;

const tools: McpToolExport['tools'] = [
  {
    name: 'places_search',
    description:
      'Find businesses and points of interest by name and/or category near a point, inside a bbox, or within a city, in the United States and the United Kingdom (Overture Maps places). Returns each place with name, category, confidence score, street address, websites, phones, emails, social links, brand and source record ids, nearest or best match first. Answers "where is the nearest Starbucks to these coordinates", "find dentists in Austin TX", "list Pret A Manger shops in London".',
    inputSchema: {
      type: 'object' as const,
      properties: {
        name: { type: 'string', description: 'Business name or words in it, e.g. "Starbucks" or "Pret A Manger". Optional when category is given.' },
        ...LOCATION_PROPS,
        ...FILTER_PROPS,
        limit: { type: 'number', description: 'Max places returned (1–200, default 25).' },
      },
      required: [],
    },
  },
  {
    name: 'places_by_domain',
    description:
      'List every physical location whose website is on a company domain — the store / office / branch footprint of a business — in the United States and the United Kingdom (Overture Maps places). Give a domain like "starbucks.com" or a full URL; subdomains and paths fold to the registrable domain. Returns the total count, counts by country and by state/region, and the locations with addresses, phones and confidence. Answers "how many locations does acme.com have and where".',
    inputSchema: {
      type: 'object' as const,
      properties: {
        domain: { type: 'string', description: 'Company domain or website URL, e.g. "starbucks.com", "https://www.tesco.com/stores".' },
        country: { type: 'string', description: 'Optional: only locations in this country ("US" or "GB").' },
        region: { type: 'string', description: 'Optional: only locations in this state/region, e.g. "TX".' },
        min_confidence: FILTER_PROPS.min_confidence,
        include_closed: FILTER_PROPS.include_closed,
        limit: { type: 'number', description: 'Max locations returned (1–1000, default 100). Counts always cover all matches.' },
      },
      required: ['domain'],
    },
  },
  {
    name: 'places_in_area',
    description:
      'Count and list the businesses and points of interest inside an area — a bbox, a radius around a point, or a whole city — in the United States and the United Kingdom (Overture Maps places), optionally for one category. Returns the total, a breakdown by category, and the highest-confidence places. Answers "how many restaurants are in this bbox", "what kinds of businesses are within 500 m of this address", "how many gyms are in Manchester".',
    inputSchema: {
      type: 'object' as const,
      properties: {
        ...LOCATION_PROPS,
        ...FILTER_PROPS,
        limit: { type: 'number', description: 'Max places listed (0–200, default 25). Counts cover the whole area.' },
      },
      required: [],
    },
  },
];

function clampLimit(v: unknown, def: number, max: number, min = 1): number {
  const n = num(v);
  if (n === null) return def;
  return Math.max(min, Math.min(max, Math.floor(n)));
}

async function placesSearch(args: Record<string, unknown>) {
  const rel = await openRelease(args);
  const f = filtersFrom(args);
  const q = typeof args.name === 'string' ? normText(args.name) : '';
  if (!q && !f.category) throw new InputError('give name (e.g. "Starbucks") and/or category (e.g. "coffee_shop")');
  const limit = clampLimit(args.limit, 25, 200);
  const area = await resolveArea(rel, args, q ? 5000 : 1000);
  if ('found' in area) return withSource(rel, area);
  const leaves = await leavesFor(rel, area);
  if (!Array.isArray(leaves)) return withSource(rel, leaves);
  const scan = leaves.slice(0, MAX_LEAVES);
  const tokens = q ? q.split(' ') : [];
  const hits: Array<{ row: Row; score: number; dist: number }> = [];
  let belowConfidence = 0;
  await mapLimit(scan, READ_CONCURRENCY, async (leaf) => {
    for (const row of await readLeaf(rel, leaf)) {
      if (row.x < area.bbox[0] || row.x > area.bbox[2] || row.y < area.bbox[1] || row.y > area.bbox[3]) continue;
      const dist = haversineM(area.center[0], area.center[1], row.y, row.x);
      if (area.radius_m !== undefined && dist > area.radius_m) continue;
      let score = 0;
      if (tokens.length) {
        const nm = normText(row.n ?? '');
        const br = row.b ? normText(row.b) : '';
        if (nm === q || br === q) score = 3;
        else if (nm.startsWith(q) || br.startsWith(q)) score = 2;
        else if (tokens.every((t) => nm.includes(t) || br.includes(t))) score = 1;
        else continue;
      }
      if (f.category && !categoryMatch(row, f.category)) continue;
      if (!passes(row, f)) {
        belowConfidence++;
        continue;
      }
      hits.push({ row, score, dist });
    }
  });
  hits.sort((a, b) =>
    b.score - a.score ||
    (area.kind === 'near' ? a.dist - b.dist : (b.row.f ?? 0) - (a.row.f ?? 0) || a.dist - b.dist),
  );
  const top = hits.slice(0, limit);
  const complete = scan.length === leaves.length;
  return withSource(
    rel,
    {
      found: hits.length > 0,
      ...(hits.length === 0
        ? {
            reason: 'no_match',
            hint: complete
              ? `No place in this area matched${belowConfidence ? `; ${belowConfidence} matched but sit below min_confidence ${f.minConf} or are closed — retry with min_confidence: 0 / include_closed: true` : ''}. Try fewer name words, a category, or a larger radius.`
              : 'The area is larger than one call scans; nothing matched in the part nearest its centre. Narrow it with near + radius_m or a smaller bbox.',
          }
        : {}),
      complete,
      ...(complete ? {} : {
        coverage_warning: `PARTIAL: scanned the ${scan.length} of ${leaves.length} data blocks nearest the centre of this area. Places further out were not searched — narrow the area (near + radius_m, a smaller bbox) for a complete answer.`,
      }),
      query: { name: args.name ?? null, area: { kind: area.kind, bbox: area.bbox, center: area.center, radius_m: area.radius_m ?? null }, ...(area.resolved ? { resolved_city: area.resolved } : {}), ...(area.alternatives ? { other_places_with_this_name: area.alternatives } : {}) },
      filters: filterSummary(f),
      matched: hits.length,
      hidden_by_filters: belowConfidence,
      returned: top.length,
      places: top.map((h) => shape(h.row, h.dist)),
    },
    top.map((h) => h.row),
  );
}

async function placesByDomain(args: Record<string, unknown>) {
  const rel = await openRelease(args);
  const raw = typeof args.domain === 'string' ? args.domain : '';
  const d = baseDomain(raw);
  if (!d) throw new InputError('domain must be a domain or URL, e.g. "starbucks.com"');
  const f = filtersFrom({ ...args, category: undefined });
  const limit = clampLimit(args.limit, 100, 1000);
  const cc = args.country ? countryCode(args.country) : null;
  if (args.country && !cc) throw new InputError('country must be an ISO code or name, e.g. "US" or "GB"');
  if (cc && !rel.manifest.countries.includes(cc)) return withSource(rel, outsideCoverage(rel, `country ${cc}`));
  const region = typeof args.region === 'string' && args.region.trim() ? args.region : null;
  if ((rel.manifest.domain_excluded ?? []).includes(d)) {
    return withSource(rel, {
      found: false,
      reason: 'platform_domain',
      domain: d,
      message: `${d} is a platform many unrelated businesses link to (social profile, link page, site builder or listing site), so it identifies no single company and is not indexed.`,
      hint: 'Pass the company\'s own website domain instead, or use places_search with the business name.',
    });
  }
  const shard = parseInt((await sha256Hex(d)).slice(0, 4), 16);
  const ent = await readIdxEntry(rel, 'domains.idx', shard);
  let rows: Row[] = [];
  if (ent.length > 0) {
    const lines = await gunzipLines(await readObject(rel.r2, binKey(rel, ent.bin), { offset: ent.offset, length: ent.length }), 'domain index');
    const prefix = `{"d":${JSON.stringify(d)},`;
    rows = lines.filter((l) => l.startsWith(prefix)).map((l) => parseLine<Row>(l, 'domain index'));
  }
  if (rows.length === 0) {
    return withSource(rel, {
      found: false,
      reason: 'no_match',
      domain: d,
      message: `No place in ${rel.manifest.countries.join(' / ')} lists a website on ${d} in Overture release ${rel.manifest.release}.`,
      hint: 'Check the domain (a parent brand may use a different one, e.g. a .co.uk site for UK stores), or search by name with places_search.',
    });
  }
  const totalListed = rows[0].dt ?? rows.length;
  const capped = totalListed > rows.length;
  let hidden = 0;
  const kept = rows.filter((r) => {
    if (cc && r.k !== cc) return false;
    if (region && !regionMatches(r.r, region)) return false;
    if (!passes(r, f)) {
      hidden++;
      return false;
    }
    return true;
  });
  kept.sort((a, b) => (b.f ?? 0) - (a.f ?? 0));
  const byCountry: Record<string, number> = {};
  const byRegion: Record<string, number> = {};
  for (const r of kept) {
    byCountry[r.k ?? 'unknown'] = (byCountry[r.k ?? 'unknown'] ?? 0) + 1;
    const rk = `${r.k ?? '?'}-${(r.r ?? '').trim() || 'unknown'}`;
    byRegion[rk] = (byRegion[rk] ?? 0) + 1;
  }
  const regionsSorted = Object.fromEntries(Object.entries(byRegion).sort((a, b) => b[1] - a[1]));
  const top = kept.slice(0, limit);
  return withSource(
    rel,
    {
      found: kept.length > 0,
      ...(kept.length === 0
        ? { reason: 'all_filtered', hint: `${hidden} location(s) list ${d} but fall below min_confidence ${f.minConf}, are closed, or are outside the country/region asked for. Retry with min_confidence: 0 or include_closed: true.` }
        : {}),
      domain: d,
      filters: { ...filterSummary(f), country: cc, region },
      locations_listing_domain: totalListed,
      ...(capped
        ? { cap_warning: `This domain is listed by ${totalListed} places; the ${rows.length} highest-confidence are indexed, so counts below cover those ${rows.length}.` }
        : {}),
      matched: kept.length,
      hidden_by_filters: hidden,
      by_country: byCountry,
      by_region: regionsSorted,
      returned: top.length,
      places: top.map((r) => shape(r)),
    },
    top,
  );
}

async function placesInArea(args: Record<string, unknown>) {
  const rel = await openRelease(args);
  const f = filtersFrom(args);
  const limit = clampLimit(args.limit, 25, 200, 0);
  const area = await resolveArea(rel, args, 1000);
  if ('found' in area) return withSource(rel, area);
  const leaves = await leavesFor(rel, area);
  if (!Array.isArray(leaves)) return withSource(rel, leaves);

  // Scan the blocks nearest the centre (that is where the listed places come
  // from). Beyond the scan budget, a block wholly inside a bbox/city area is
  // counted from the index — exact under the default filters, because the
  // index stores each block's count at confidence >= 0.7 and not closed. Only
  // boundary blocks past the budget are genuinely unknown, and those make the
  // answer PARTIAL, said in the payload, never silently.
  const scan = leaves.slice(0, MAX_LEAVES);
  const rest = leaves.slice(MAX_LEAVES);
  const indexable = area.kind !== 'near' && isDefaultFilter(f);
  const insideByIndex = indexable ? rest.filter((l) => contains(area.bbox, l.bounds)) : [];
  const unread = rest.filter((l) => !insideByIndex.includes(l));
  const complete = unread.length === 0;
  const indexCount = insideByIndex.reduce((s, l) => s + l.e[4], 0);

  let counted = indexCount;
  let hidden = 0;
  const byCat: Record<string, number> = {};
  const rows: Array<{ row: Row; dist: number }> = [];
  await mapLimit(scan, READ_CONCURRENCY, async (leaf) => {
    for (const row of await readLeaf(rel, leaf)) {
      if (row.x < area.bbox[0] || row.x > area.bbox[2] || row.y < area.bbox[1] || row.y > area.bbox[3]) continue;
      const dist = haversineM(area.center[0], area.center[1], row.y, row.x);
      if (area.radius_m !== undefined && dist > area.radius_m) continue;
      if (f.category && !categoryMatch(row, f.category)) continue;
      if (!passes(row, f)) {
        hidden++;
        continue;
      }
      counted++;
      const c = row.c ?? 'uncategorized';
      byCat[c] = (byCat[c] ?? 0) + 1;
      rows.push({ row, dist });
    }
  });
  rows.sort((a, b) => (b.row.f ?? 0) - (a.row.f ?? 0) || a.dist - b.dist);
  const top = rows.slice(0, limit);
  const unreadUpper = isDefaultFilter(f) ? unread.reduce((s, l) => s + l.e[4], 0) : null;
  const categories = Object.entries(byCat).sort((a, b) => b[1] - a[1]);
  return withSource(
    rel,
    {
      found: counted > 0,
      ...(counted === 0
        ? { reason: 'no_match', hint: complete ? `Nothing in this area passes the filters${hidden ? ` (${hidden} hidden by min_confidence / closed status / category)` : ''}.` : 'Nothing in the scanned part of this area — narrow it.' }
        : {}),
      complete,
      area: { kind: area.kind, bbox: area.bbox, center: area.center, radius_m: area.radius_m ?? null, ...(area.resolved ? { resolved_city: area.resolved } : {}), ...(area.alternatives ? { other_places_with_this_name: area.alternatives } : {}) },
      filters: filterSummary(f),
      total: complete
        ? { count: counted, exact: true }
        : {
            count: null,
            exact: false,
            at_least: counted,
            at_most: unreadUpper === null ? null : counted + unreadUpper,
            note: unreadUpper === null
              ? `PARTIAL: ${unread.length} of ${leaves.length} data blocks were not scanned, and with non-default filters they cannot be counted from the index. Narrow the area for an exact count.`
              : `PARTIAL: ${unread.length} boundary blocks were not scanned; their places are counted in at_most only. Narrow the area for an exact count.`,
          },
      by_category: Object.fromEntries(categories.slice(0, 40)),
      ...(insideByIndex.length
        ? { by_category_scope: `by_category covers the ${counted - indexCount} places in the ${scan.length} scanned blocks nearest the centre; another ${indexCount} places in ${insideByIndex.length} blocks wholly inside the area are included in total but have no category breakdown. For an exact count of one category, pass category with a smaller area.` }
        : {}),
      categories_seen: categories.length,
      hidden_by_filters: hidden,
      blocks: { total: leaves.length, counted_from_index: insideByIndex.length, scanned: scan.length, not_scanned: unread.length },
      returned: top.length,
      places: top.map((h) => shape(h.row, area.kind === 'near' ? h.dist : undefined)),
    },
    top.map((h) => h.row),
  );
}

async function callTool(name: string, args: Record<string, unknown>): Promise<unknown> {
  try {
    switch (name) {
      case 'places_search':
        return await placesSearch(args);
      case 'places_by_domain':
        return await placesByDomain(args);
      case 'places_in_area':
        return await placesInArea(args);
      default:
        throw new Error(`Unknown tool: ${name}`);
    }
  } catch (e) {
    if (e instanceof InputError) {
      return { ...sourceBlock(null), found: false, reason: 'invalid_input', message: e.message, hint: 'See the tool input schema; examples are in the tool listing.' };
    }
    throw e;
  }
}

export default { tools, callTool, provider: 'overture-maps' } satisfies McpToolExport;
