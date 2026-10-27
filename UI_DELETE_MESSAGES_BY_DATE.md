# UI Guide: Delete Messages in One Chat (Date Range + Single Message)

This document is for frontend development. It describes how to add **date-wise message delete** inside an **open chat**, without affecting any other chat.

## Product behaviour

| Action | Scope | What happens |
|---|---|---|
| Delete by date range | **Current chat only** | Messages whose `timestamp` falls between start and end are removed. All other messages in that chat stay. Other chats are never touched. |
| Delete one message | **Current chat only** | One message is removed. |
| Delete entire chat | Whole chat | Existing API. Removes the chat and **all** of its messages. Use this from the chat list, not from the date-range dialog. |

**Do not** send a global “delete by date” request with no `chatId`. Every delete in this flow must include the open chat’s id.

---

## Where it lives in the UI

Place the control **inside the open chat screen**, in the header above the conversation (next to the chat name / more menu).

Recommended layout:

1. User opens **one** chat from the chat list.
2. Header shows chat name, participants, and a **⋯** or **Delete by date** action.
3. Tapping that action opens a **modal / bottom sheet** that belongs to this chat only.
4. Modal contains:
   - Chat name (so the user sees which thread they are editing)
   - Start date picker
   - End date picker
   - Optional live count: “X messages will be deleted”
   - Cancel
   - Delete (destructive, confirm again)
5. After success, keep the same chat open and **reload messages**. Do not navigate away unless the chat is now empty and you choose to.

Suggested copy:

- Button: `Delete messages by date`
- Modal title: `Delete messages in “{chatName}”`
- Helper: `Only this chat is affected. Messages outside the selected dates stay.`
- Confirm: `Delete {count} messages from {startDate} to {endDate}? This cannot be undone.`

---

## Auth (all endpoints)

Send the logged-in JWT on every request:

```http
Authorization: Bearer <token>
Content-Type: application/json
```

If the token is missing or invalid, the API returns `401`:

```json
{ "error": "Unauthorized" }
```

or

```json
{ "error": "Invalid token" }
```

UI should send the user back to login.

Base URL (local): `http://localhost:5000`

---

## Step 1 — Open chat and load messages (already exists)

```http
GET /api/chats/:chatId
Authorization: Bearer <token>
```

Response:

```json
{
  "id": 12,
  "user_id": 1,
  "name": "Family",
  "created_at": "2025-03-01T10:00:00.000Z",
  "participants": ["Amit", "Riya"]
}
```

```http
GET /api/chats/:chatId/messages
Authorization: Bearer <token>
```

Optional query: `?search=hello`

Response:

```json
{
  "items": [
    {
      "id": 101,
      "author": "Amit",
      "content": "Hi",
      "timestamp": "2025-04-10T09:15:00.000Z",
      "type": "text",
      "media_path": null
    }
  ],
  "total": 1840
}
```

Use `chatId` from this screen for every delete call below.

---

## Step 2 — Bound the date pickers (new)

Call this when the date-range modal opens. Use min/max so the user cannot pick dates with no history.

```http
GET /api/chats/:chatId/messages/date-range
Authorization: Bearer <token>
```

Success `200`:

```json
{
  "chatId": 12,
  "minTimestamp": "2024-01-05T08:12:00.000Z",
  "maxTimestamp": "2026-09-18T21:40:00.000Z",
  "total": 1840,
  "nullTimestampCount": 0
}
```

UI notes:

- Set date-picker **min** to `minTimestamp` (date part).
- Set date-picker **max** to `maxTimestamp` (date part).
- Default **start date** = `minTimestamp` date, **end date** = `maxTimestamp` date (or leave empty until the user picks).
- If `total` is `0`, disable Delete and show “No messages in this chat”.
- `nullTimestampCount` messages have no timestamp and **will not** be removed by date-range delete.

---

## Step 3 — Preview how many messages will be deleted (new, recommended)

Call this whenever start or end date changes (debounce ~300ms).

```http
GET /api/chats/:chatId/messages/by-date?startDate=2025-01-01&endDate=2025-01-31
Authorization: Bearer <token>
```

Success `200`:

```json
{
  "chatId": 12,
  "startDate": "2025-01-01 00:00:00",
  "endDate": "2025-01-31 23:59:59",
  "matchingCount": 86
}
```

UI notes:

- Show `matchingCount` in the modal: `86 messages in this range`.
- Disable the Delete button when `matchingCount === 0`.
- This request **does not delete** anything.

---

## Step 4 — Delete messages in the selected date range (new)

Use this only after a confirmation dialog.

```http
DELETE /api/chats/:chatId/messages/by-date
Authorization: Bearer <token>
Content-Type: application/json
```

Body:

```json
{
  "startDate": "2025-01-01",
  "endDate": "2025-01-31"
}
```

Dates can also be sent as query params:

```http
DELETE /api/chats/:chatId/messages/by-date?startDate=2025-01-01&endDate=2025-01-31
```

Accepted date formats:

| Input | Treated as |
|---|---|
| `2025-01-01` | start → `2025-01-01 00:00:00`, end → `2025-01-31 23:59:59` (inclusive full days) |
| `2025-01-01 09:00` | exact local datetime (`:00` seconds added) |
| `2025-01-01 09:00:00` | exact local datetime |
| ISO datetime | parsed to local server datetime |

For a typical calendar UI, send **date-only** `YYYY-MM-DD`. That deletes the whole selected days, inclusive.

Aliases also accepted in body/query: `start_date` / `end_date`, or `from` / `to`.

Success `200`:

```json
{
  "ok": true,
  "chatId": 12,
  "startDate": "2025-01-01 00:00:00",
  "endDate": "2025-01-31 23:59:59",
  "deleted": 86,
  "deletedMedia": 4,
  "remaining": 1754
}
```

After success:

1. Close the modal.
2. Toast: `Deleted 86 messages`.
3. Re-fetch `GET /api/chats/:chatId/messages`.
4. Optionally refresh the chat list so `messageCount` updates (`GET /api/chats`).
5. Do **not** remove the chat from the list. The chat still exists with remaining messages.

Media files attached to deleted messages are removed on disk as well (`deletedMedia`).

---

## Optional: delete one message in the open chat (new)

If you add long-press / swipe-to-delete on a bubble:

```http
DELETE /api/chats/:chatId/messages/:messageId
Authorization: Bearer <token>
```

Success `200`:

```json
{
  "ok": true,
  "chatId": 12,
  "messageId": 101,
  "deleted": 1,
  "deletedMedia": 0
}
```

Remove that bubble from local state, or reload the thread. This still only affects the open chat.

---

## Existing: delete the whole chat (chat list)

This is **not** the date-range feature. Use it only when the user wants the entire conversation gone.

```http
DELETE /api/chats/:chatId
Authorization: Bearer <token>
```

Success: `{ "ok": true }`

Then navigate back to the chat list.

---

## Error handling

| Status | Body | UI |
|---|---|---|
| 400 | `{ "error": "startDate and endDate are required (YYYY-MM-DD or datetime)" }` | Keep modal open, show the message |
| 400 | `{ "error": "startDate must be before or equal to endDate" }` | Highlight the date fields |
| 400 | `{ "error": "Invalid chat id" }` | Should not happen if you use numeric ids from the API |
| 401 | `{ "error": "Unauthorized" }` / `{ "error": "Invalid token" }` | Redirect to login |
| 404 | `{ "error": "Chat not found" }` | Chat was deleted or not owned; go back to list |
| 404 | `{ "error": "Message not found" }` | Message already gone; refresh thread |
| 500 | `{ "error": "..." }` | Generic error toast |

---

## Frontend flow (recommended)

```text
Open Chat (chatId)
  └─ Header → "Delete messages by date"
        ├─ GET /api/chats/:chatId/messages/date-range
        │     bound pickers to min/max
        ├─ User picks startDate + endDate
        ├─ GET /api/chats/:chatId/messages/by-date?startDate&endDate
        │     show matchingCount
        ├─ Confirm dialog
        ├─ DELETE /api/chats/:chatId/messages/by-date
        └─ Reload GET /api/chats/:chatId/messages
```

Example (fetch):

```js
const token = localStorage.getItem('token');
const chatId = 12;

async function previewDelete(startDate, endDate) {
  const qs = new URLSearchParams({ startDate, endDate });
  const res = await fetch(`/api/chats/${chatId}/messages/by-date?${qs}`, {
    headers: { Authorization: `Bearer ${token}` }
  });
  return res.json();
}

async function deleteByDate(startDate, endDate) {
  const res = await fetch(`/api/chats/${chatId}/messages/by-date`, {
    method: 'DELETE',
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ startDate, endDate })
  });
  return res.json();
}
```

React-style date state:

```js
const [startDate, setStartDate] = useState(''); // "YYYY-MM-DD"
const [endDate, setEndDate] = useState('');
```

HTML date inputs already emit `YYYY-MM-DD`, which the API accepts.

---

## Rules the UI must follow

1. Always use the **open chat’s id**. Never apply one date range to every chat.
2. Require both dates before enabling Delete.
3. If `startDate > endDate`, swap them or block submit (API will also reject this).
4. Always confirm before DELETE. The operation cannot be undone.
5. After delete, refresh **this** chat’s messages. Other chats in the sidebar stay as they are.
6. Date-range delete does not delete messages with `timestamp = null`.
7. Deleting all messages in a range does **not** delete the chat itself. Empty chats remain until the user deletes the chat from the list.

---

## Quick API index for this feature

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/chats/:id/messages/date-range` | Min/max timestamps for pickers |
| GET | `/api/chats/:id/messages/by-date?startDate&endDate` | Preview count (no delete) |
| DELETE | `/api/chats/:id/messages/by-date` | Delete messages in that chat + date range |
| DELETE | `/api/chats/:id/messages/:messageId` | Delete one message in that chat |
| GET | `/api/chats/:id/messages` | Reload thread after delete |
| DELETE | `/api/chats/:id` | Delete entire chat (separate feature) |
