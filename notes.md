# SLIP-Message Notes

- body can be json, sen, or lisp. If first character is ( then lisp else sen
- subscribers are instances
 - maybe msg-subscribe create a subscriber
  - close to stop listening
  - provide callback that takes subscriber, message, and message id
   - ack with message id when processed
   - if message id is nil then no ack needed
- subject identifies a stream
 - manage stream characteristics separately
- msg-hub - abstract for message hub (msg-service?) not needed, duck typing is enough
 - publish
 - request (request reply)
 - subscribe (hub subject callback &keys :content-type [:raw :json :lisp nil])
  - if callback is nil then expect the user to (send subscriber :next)
 - receive - receive on from stream, with timeout
 - close
 -
- jetstream-hub-flavor
- local-hub-flavor or process-hub-flavor

- subject configuration out of band possibly
 - will a simple string be enough or are jetstream variations needed?
- publish or send is always the same
 - handling is configured out of band or through the hub
- listen with callback
- (:get subject) for queues
- explicit ack if configured
