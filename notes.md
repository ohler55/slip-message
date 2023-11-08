# SLIP-Message Notes

- message-subscriber-flavor
 - subscribe or make-instance 'message-subscriber-flavor :hub hub :subject subject ...)
  - two ways to make (or 3?)
   (send hub :subscribe ...)
   (message-subscribe hub subject ...)
   (make-instance 'message-subscriber-flavor :hub hub :subject subject ...)

- hub flavor
 - :configure-subject - with parameters, use jetstream model
 - :remove-subject
 - :publish (subject content &optional content-type)
  - content can be bag [json or sen], lisp [sexp], string [raw bytes]
  - content-type :json, :sen, :lisp, :raw, nil
   - nil uses :json for bag, :lisp for lisp, and raw for string, others print/append
 - :subscribe (subject callback &optional content-type) => msg-subscriber-flavor instance
  - callback of nil waits for (send subscriber :next timeout)
   - callback (subscriber msg-id msg error)
    - should msg-id be context instead then (send subscriber :ack context)
 - :close - shutdown hub
 - :request (subject message &optional content-type) => reply (bag, lisp, raw)

- local-hub-flavor
 -
 - configureable
  - basic delivers to current subscribers
   - list of subscribers (which have subject/filter)
   - subscriber can queue with sub-subscribers
 - on publish verify subject
  - walk subscribers and for any subject match copy message and use callback
   - convert to correct format if possible else callback with error


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
