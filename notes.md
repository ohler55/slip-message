# SLIP-Message Notes

- todo
 - app-hub
  - queue returns an assoc
  - subscriber ack
   - app-hub ack (consumer message-id)

 - queues
  - subscriber :next
   - calls to hub

 - queue interface
  - push msg
   - create id for message
  - next (name) => msg, msg-id
  - ack (name msg-id)
 - work-queue
  - push
   - put on single stack of workEnv (raw-msg, msg-id, envStatus [new, pending, acked])
  - next
   - set pending on next available
  - ack
   - match msg and set acked
   - head then slide until msg that is not acked
 - all-queue (basically persistent)
  - push
   - push onto stack of (msg-id, raw-msg, map[name]envStatus)
  - next
   - find envelope in map for name, if acked already try next one, if pending assume acked
  - ack
   - mark by removing from map
   - if map empty then update stack as needed

- jetstream-hub-flavor
