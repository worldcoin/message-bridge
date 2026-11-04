# Message Bridge (previously Wallet Bridge)

The message bridge is a **dumb**, environment and client agnostic relay of arbitrary messages. It lets two parties share an arbitrary message where parties can gossip a 
symmetric key off-band.

The bridge is made so that it cannot eavesdrop on any messages. The bridge expects only ciphertext. **All clients MUST encrypt all payloads before submitting to the bridge.**.

- **Importantly** this bridge is completely agnostic to any client or environment, it simply relays messages, hence it implements no logic or opinions related to client handling.
- All messages received by this bridge are temporarily held for delivery, but are automatically purged after a period of time. This service has and SHOULD NOT have _persisting_ storage.

## Use Case: World ID Protocol

The bridge is currently used in the [World ID Protocol](https://github.com/worldcoin/world-id-protocol). The most used path is for RPs to request proofs from users (to their Authenticators) and for Authenticators to send back proofs.

### Example Flow

```mermaid
sequenceDiagram
IDKit ->> Bridge: POST /request
Bridge ->> IDKit: <id>
IDKit ->> Bridge: Poll for updates GET /response/:id
Authenticator ->> Bridge: GET /request/:id
Bridge ->> Authenticator: <request>
Authenticator ->> Bridge: PUT /response/:id
IDKit ->> Bridge: Poll for updates GET /response/:id
Bridge ->> IDKit: <response>
```

### Endpoints

- `POST /request`: Called by IDKit. Initializes a proof verification request.
- `GET /request/:id`: Called by Authenticator. Used to fetch the proof verification request. One time use.
- `HEAD /request/:id`: Existence check for a request. `200` if present, `404` otherwise.
- `PUT /response/:id`: Called by Authenticator. Used to send the proof back to the application.
- `GET /response/:id`: Called by IDKit. Continuous pulling to fetch the status of the request and the response if available. Response can only be retrieved once.
- `HEAD /response/:id`: Existence check for a request's status. `200` if present, `404` otherwise.
- `PUT /request/:id`: Staging only (`ENVIRONMENT == "staging"`). Fully resets the session at this ID, intentionally bypassing reservations.

Response uploads may include an optional `tracking_receipt` alongside `iv` and
`payload`. The receipt remains opaque, expires with the response, and is never
returned to the response consumer. When `ANALYTICS_CALLBACK_URL` is set, the bridge
sends the receipt to that URL after the response is dequeued and decoded. Leave the variable unset to disable
callbacks; a configured URL must be valid (an empty value is invalid). Analytics
delivery is best-effort and never blocks response delivery. Operational metrics
and tracing remain enabled independently of callbacks.

### Response-read protection

Set `require_token_to_read_response: true` on `POST /request` to receive a private
`response_read_token`. Send it as `Authorization: Bearer <token>` on every
`GET /response/:id`. Missing or invalid tokens return `401` without consuming the
response. Without the opt-in, response reads remain public. Request GET and
response PUT remain unauthenticated; keep the token out of shared URLs.

Deploy Bridge and drain old instances before releasing clients that require tokens.
This protects response reads; it does not prevent request enumeration or consumption.

### ID reservations

IDs stay reserved for 45 minutes; request, response, and status data expire after
15 minutes. Consumption and polling do not release or extend the reservation.
`POST /request` returns `409` for occupied IDs; generated IDs retry on collision.
Staging `PUT /request/:id` intentionally resets the session.

Creation atomically stores the reservation and payload, clearing stale legacy data.
Legacy telemetry correlation remains supported until an ID is reused.
Drain old instances before relying on reservations. Delayed writes after expiry can still affect a reused ID.

## Local Development

An easy way to run is using a Dockerized Redis:

```
docker run -d -p 6379:6379 redis
```

When building the Dockerfile locally remember to specify the `--platform=linux/amd64` flag.

## Testing

Integration tests build the bridge in-process and drive it directly, so the only external dependency is Redis (override its location with `REDIS_URL`):

```bash
docker-compose -f docker-compose.test.yml up -d
cargo test
```
