# Input files travel as multipart form data

A submission's Input files are files: each is `{ path, content }`, its content a `File` of any bytes. A request that carries one is sent as `multipart/form-data`, which oRPC's OpenAPI handler and link read and write in bracket notation (`jobs[0][files][0][content]`); a request without one stays JSON. The generated OpenAPI document gives the operation both media types. The CLI, whose JSON input has no bytes, turns each file's text into a `File` before sending.

The hub reads a whole request into memory before validating it, so it caps a request body at 16 MiB with oRPC's `RequestLimitHandlerPlugin`. On Node, parsing a multipart body peaked at about 2 to 4.5 times its size in extra memory (a 32 MiB file at about 140 MiB, 100 files totalling 32 MiB at about 73 MiB). The cap is provisional.

## Considered options

- Base64 content in JSON: a third larger on the wire, and the hub would hold the encoded string beside the decoded bytes; every caller encodes and decodes by hand.
- Uploading each file first and naming it in the submission: no body holds more than one file, and a file a Batch shares is sent once, but every submission takes several requests and the hub needs a store for uploads not yet used. It remains the way past the body cap.
- oRPC's own RPC protocol, which keeps types inside a multipart body: callers other than oRPC's client would have no documented format to write.

## Consequences

Bracket notation has no way to write an empty object, an empty array or `null`, so a multipart request loses them: a missing `files` is read as none, and a Batch's Output contract sent together with its agent's files loses any empty object or array in its schema. oRPC's `SmartCoercionHandlerPlugin` turns the fields' text back into the numbers and booleans the contract expects. Paths are checked as the request is validated, and the Runner checks again that each file lands inside `inputs/` before writing it. Reconsider once files are uploaded before the submission that names them, or when a request needs more than the cap.
