# @pipeworx/overture-places

Business and point-of-interest locations in the United States and the United
Kingdom, from the Overture Maps Foundation places theme. Each place carries a
name, category, existence confidence score, street address, websites, phones,
emails, social links, brand and contributing source record ids. Use it to find
places near a point or in a city, to map every location of a company by its
website domain, and to count what is in an area.

Part of [Pipeworx](https://pipeworx.io) — an MCP gateway connecting AI agents to 1856+ live data sources. This is an independent, unofficial integration — not affiliated with, endorsed by, or published by the upstream provider.

## Tools

- `places_search(name?, category?, near{lat,lon} + radius_m | bbox | city + country [+ region], min_confidence?, include_closed?, limit?)`:
  businesses matching a name and/or category. Results are ranked by match
  quality and then by distance (for `near`) or confidence. Example: the
  Starbucks branches within 3 km of downtown Austin.
- `places_by_domain(domain, country?, region?, min_confidence?, include_closed?, limit?)`:
  every location whose website is on a company's domain, which gives that
  company's physical footprint. Subdomains and paths fold to the registrable
  domain (`https://www.starbucks.com/store-locator` → `starbucks.com`). Returns
  the total plus counts by country and by state/region.
- `places_in_area(near + radius_m | bbox | city + country, category?, min_confidence?, include_closed?, limit?)`:
  the count of places in an area, a breakdown by category, and the
  highest-confidence places.

Every response starts with `attribution`, a `source` block (licence, release,
terms URL) and `data_as_of`, which is the Overture release the answer came
from (for example `2026-09-23.1`).

## Confidence

Overture gives every place an existence confidence from 0 to 1 and calls it
"the primary tool for filtering potential junk data". **By default this pack
hides places below 0.7, and places marked `permanently_closed`.** In the
measured sample the distribution breaks sharply at 0.7, and the rows below it
were mostly junk. The default keeps about 68% of places. Every place shows its
`confidence`, and every response states the threshold it applied and how many
places it hid (`hidden_by_filters`). To see everything, pass
`min_confidence: 0` and `include_closed: true`.

## Coverage and limits

- **Countries: US and GB.** An area or city outside them returns
  `found:false, reason:"outside_coverage"` and lists the covered countries.
- **One call scans at most 64 data blocks**, nearest the centre of the area
  first. That is about 250,000 places. For a larger area (a whole metro
  searched by name), the response sets `complete:false` and explains in
  `coverage_warning` what was not searched. `places_in_area` still gives exact
  or bounded totals from its index. Narrow the area with `near` + `radius_m`
  for a complete answer.
- **City lookup** resolves a locality name from the addresses in the data. The
  `region` field is consistent in the US (state codes; full state names are
  accepted too) but inconsistent in the UK, so leave `region` out for UK
  cities. A name that matches several places (several Springfields) uses the
  largest and lists the others in `other_places_with_this_name`.
- **Platform domains are not indexed.** Social profiles, link pages, site
  builders and listing sites (instagram.com, linktr.ee, wixsite.com, yelp.com
  and similar) identify no single company. `places_by_domain` refuses them
  with `reason:"platform_domain"`.
- **Very large domains:** the 5,000 highest-confidence locations per domain
  are indexed. The response always gives the true total in
  `locations_listing_domain` and says when the cap applied.
- Overture warns that the theme "is known to contain duplicates, a high junk
  rate, and low property completeness". Confidence filtering helps with junk
  but does not remove duplicates.

## Auth

Keyless. The pack answers only through the Pipeworx gateway
(`https://gateway.pipeworx.io/mcp`). A standalone install has no Overture
release to read, so every call fails with an error that says so.

## Data sources

- Overture Maps Foundation places theme, monthly GeoParquet releases:
  <https://docs.overturemaps.org/guides/places/>, at
  `s3://overturemaps-us-west-2/release/<release>/theme=places/type=place/`.
  The `overture-places-ingest` workflow checks for a new release every week
  (design: `docs/overture-places-design.md`).

### Licence and attribution

The places theme is published under **CDLA-Permissive-2.0** and
**Apache-2.0**, depending on the source. Redistribution is permitted, including
commercial use, with attribution. Overture states that the theme contains no
OpenStreetMap data, so ODbL does not apply. Attribution, as Overture publishes
it (<https://docs.overturemaps.org/attribution/>):

> © Overture Maps Foundation (https://overturemaps.org), places theme.
> Data from Meta. Available under CDLA Permissive 2.0.
> Data from Microsoft. Available under CDLA Permissive 2.0.
> Data from PinMeTo. Available under CDLA Permissive 2.0.
> Data from Krick. Available under CDLA Permissive 2.0.
> Data from RenderSEO. Available under CDLA Permissive 2.0.
> Data from DAC. Available under CDLA Permissive 2.0.
> Data from BrightQuery. Available under CDLA Permissive 2.0.
> Data from Foursquare. Copyright 2024 Foursquare Labs, Inc. All rights reserved. Available under Apache 2.0. Foursquare data was transformed to the Overture schema.
> Data from AllThePlaces. Available under CC0 1.0.

Each response's `attribution` names the Overture release and the per-source
credit lines for the sources in that response. Each place lists its own
contributing datasets in `sources` and their licences in `source_licenses`.

## Quick Start

Add to your MCP client (Claude Desktop, Cursor, Windsurf, etc.):

```json
{
  "mcpServers": {
    "overture-places": {
      "url": "https://gateway.pipeworx.io/overture-places/mcp"
    }
  }
}
```

### What this endpoint actually serves

`tools/list` at `https://gateway.pipeworx.io/overture-places/mcp` returns the tools in the table
above **plus the shared Pipeworx meta-tools** — `ask_pipeworx`,
`discover_tools`, `search_within`, `remember`/`recall` and the rest of the
gateway-wide set. So the tool count you see is larger than this table: a
single-pack endpoint currently lists roughly 30 shared tools alongside the
pack's own. The connection's `initialize` response states its exact scope, and
is the authoritative answer for a given day.

This is deliberate, not multiplexing by accident. The meta-tools are what let a
scoped connection answer a question this pack does not cover — via
`ask_pipeworx`, which routes across the whole catalog — without you adding a
second MCP server. There is currently no way to mount a pack endpoint without
them; if the extra schemas cost you more context than the routing is worth,
connect to the full gateway once rather than to several pack endpoints.

Or connect to the full Pipeworx gateway to get every pack's tools listed
directly, instead of just this one's:

```json
{
  "mcpServers": {
    "pipeworx": {
      "url": "https://gateway.pipeworx.io/mcp"
    }
  }
}
```

Both URLs reach the same gateway and the same 1856+ data sources. The
only difference is which pack's tools are listed **directly**; `ask_pipeworx`
reaches all of them from either one.

## No MCP client? Call it over HTTP

```bash
curl -X POST https://gateway.pipeworx.io/v1/tools/places_search \
  -H 'Content-Type: application/json' \
  -d '{"name":"Starbucks","near":{"lat":30.2672,"lon":-97.7431},"radius_m":3000,"limit":5}'
```

No account needed for the first calls. Inspect any tool: `GET https://gateway.pipeworx.io/v1/tools/places_search`. Find one: `POST https://gateway.pipeworx.io/v1/tools/search_packs` with `{"query":"..."}`.

## Run it from npm

`@pipeworx/mcp-overture-places` is a local stdio bridge to the Pipeworx gateway: your MCP client
starts it with `npx`, and it forwards every call to the same
`https://gateway.pipeworx.io/overture-places/mcp` endpoint shown above. It needs network access;
there is no offline mode.

```json
{
  "mcpServers": {
    "overture-places": {
      "command": "npx",
      "args": ["-y", "@pipeworx/mcp-overture-places"]
    }
  }
}
```

Without a key it calls anonymously, capped at 50 calls a day. A free account
raises that to 200 a day: get a key at https://pipeworx.io/signup and add
`"env": { "PIPEWORX_API_KEY": "<your key>" }` to the server entry.
The key is sent as a Bearer token, the same way a hosted connection sends it,
so calls are metered and billed exactly as they are there.

It lists **only** this pack's tools by default. Set `PIPEWORX_META_TOOLS=1` to
also list `ask_pipeworx` and the other gateway-wide tools.

Behind an HTTPS proxy: Node's built-in `fetch` does not read `HTTPS_PROXY` by
default. Newer Node releases honour it when `NODE_USE_ENV_PROXY=1` is set; check
your Node version's documentation.

## Using with ask_pipeworx

Instead of calling tools directly, you can ask questions in plain English —
this works on the pack endpoint above as well as on the full gateway:

```
ask_pipeworx({ question: "your question about Overture Places data" })
```

The gateway picks the right tool and fills the arguments automatically.

## More

- [Docs and guides](https://pipeworx.io/docs)
- [pipeworx.io](https://pipeworx.io)

## License

MIT
