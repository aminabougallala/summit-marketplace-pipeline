# TrailMart partner API

Base URL inside the Summit network: `http://trailmart-api.confluent.svc:8080`

## GET /v1/settlements

Returns the settlements of one calendar day for the partner account.

| Parameter | Required | Meaning |
|---|---|---|
| `date` | yes | The settlement day, `YYYY-MM-DD`. |
| `page_size` | no | Rows per response, 1 to 100 (default 100). |
| `cursor` | no | The `next_cursor` value of the previous response. Omit it for the first page. |

Response body:

```json
{
  "settlements": [ { "settlement_id": "...", "order_ref": "...", "settlement_date": "2026-10-26",
                     "category": "...", "quantity": 2, "gross_amount": 84.10,
                     "marketplace_fee": 8.41, "net_amount": 75.69 } ],
  "next_cursor": "64",
  "total_count": 1216
}
```

`next_cursor` is `null` on the last page. `total_count` is the number of
settlements for the day across all pages.

## Change log

### 2026-10-26 · Cursor-based paging (API v2 rollout complete)

Paging moved from page numbers to cursors on all partner accounts.

- The `page` parameter is **ignored**. Every request without a `cursor` returns the first page.
- The `total_pages` field is **no longer returned**. Use `next_cursor` to continue and `total_count` to check completeness.
- Response shape otherwise unchanged.

Partners were notified by e-mail on 2026-10-12 (ticket TM-4471).

### 2025-03-01 · Initial release (v1)

Page-number paging: `page` and `page_size` parameters; the response carried
`page`, `total_pages` and `total_count`.
