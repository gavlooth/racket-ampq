#lang racket

(require ffi/unsafe)
(require describe)
(require setup/dirs)
(require libuuid)


(define SEQ (string->bytes/utf-8 "sequence"))

(file-name-from-path (car  (string-split (getenv "LD_LIBRARY_PATH") ":")))

(define (locate-proto-path)
  (let ([paths (map string->path
                  (map (λ (x)
                         (if (string-suffix? x "/")
                           (format "~alibqpid-proton.so" x)
                           (format "~a/libqpid-proton.so" x)))
                       (string-split (getenv "LD_LIBRARY_PATH") ":")))])
    (ormap (λ (x) (if (file-exists? x) x #f) ) paths)))


(define pn_event_type_t
 (_enum
  '(PN_EVENT_NONE  PN_REACTOR_INIT PN_REACTOR_QUIESCED PN_REACTOR_FINAL
    PN_TIMER_TASK PN_CONNECTION_INIT PN_CONNECTION_BOUND
    PN_CONNECTION_UNBOUND PN_CONNECTION_LOCAL_OPEN PN_CONNECTION_REMOTE_OPEN
    PN_CONNECTION_LOCAL_CLOSE PN_CONNECTION_REMOTE_CLOSE PN_CONNECTION_FINAL
    PN_SESSION_INIT PN_SESSION_LOCAL_OPEN PN_SESSION_REMOTE_OPEN
    PN_SESSION_LOCAL_CLOSE PN_SESSION_REMOTE_CLOSE PN_SESSION_FINAL
    PN_LINK_INIT PN_LINK_LOCAL_OPEN PN_LINK_REMOTE_OPEN
    PN_LINK_LOCAL_CLOSE PN_LINK_REMOTE_CLOSE PN_LINK_LOCAL_DETACH
    PN_LINK_REMOTE_DETACH PN_LINK_FLOW PN_LINK_FINAL PN_DELIVERY
    PN_TRANSPORT PN_TRANSPORT_AUTHENTICATED PN_TRANSPORT_ERROR
    PN_TRANSPORT_HEAD_CLOSED PN_TRANSPORT_TAIL_CLOSED PN_TRANSPORT_CLOSED
    PN_SELECTABLE_INIT PN_SELECTABLE_UPDATED PN_SELECTABLE_READABLE
    PN_SELECTABLE_WRITABLE PN_SELECTABLE_ERROR PN_SELECTABLE_EXPIRED
    PN_SELECTABLE_FINAL PN_CONNECTION_WAKE PN_LISTENER_ACCEPT
    PN_LISTENER_CLOSE PN_PROACTOR_INTERRUPT PN_PROACTOR_TIMEOUT
    PN_PROACTOR_INACTIVE PN_LISTENER_OPEN PN_RAW_CONNECTION_CONNECTED
    PN_RAW_CONNECTION_CLOSED_READ PN_RAW_CONNECTION_CLOSED_WRITE
    PN_RAW_CONNECTION_DISCONNECTED PN_RAW_CONNECTION_NEED_READ_BUFFERS
    PN_RAW_CONNECTION_NEED_WRITE_BUFFERS PN_RAW_CONNECTION_READ
    PN_RAW_CONNECTION_WRITTEN PN_RAW_CONNECTION_WAKE)))

(define proto-lib  (ffi-lib (locate-proto-path)))

(define build-proactor-address (get-ffi-obj "pn_proactor_addr" proto-lib (_fun _pointer _int _pointer _pointer  ->  _int)))

(define _pn_proactor_t (_cpointer 'pn_proactor_t))

(define _pn_event_batch_t (_cpointer '_pn_event_batch_t))

(define _pn_event_t (_cpointer '_pn_event_t))

(define _pn_message_t (_cpointer 'pn_message_t))

(define _pn_data_t  (_cpointer 'pn_data_t))

(define _pn_message (_cpointer 'pn_message))

(define _pn_bytes_t (_cpointer 'pn_bytes_t))

(define  pn_session_t (_cpointer 'pn_session_t))

(define pn_connection_t  (_cpointer 'pn_connection_t))

(define pn-message (get-ffi-obj "pn_message" proto-lib (_fun -> _pn_message_t)))

(define pn-data (get-ffi-obj "pn_data" proto-lib (_fun _int -> _pn_data_t)))

(define pn-data-enter (get-ffi-obj "pn_data_enter" proto-lib (_fun _pn_data_t -> _bool)))

(define pn-message-id (get-ffi-obj "pn_message_id" proto-lib (_fun _pn_message_t -> _pn_data_t)))

(define pn-message-clear  (get-ffi-obj "pn_message_clear" proto-lib  (_fun _pn_message_t -> _void)))

(define pn-message-body  (get-ffi-obj "pn_message_body" proto-lib  (_fun _pn_message_t -> _pn_data_t)))

(define pn-data-put-map (get-ffi-obj "pn_data_put_map" proto-lib  (_fun _pn_data_t -> _int)))

(define pn-bytes (get-ffi-obj "pn_bytes" proto-lib  (_fun _int _string -> _pn_bytes_t)))

(define pn-data-put-string (get-ffi-obj "pn_data_put_string" proto-lib  (_fun _pn_data_t _string  -> _int)))

(define pn-data-put-int (get-ffi-obj "pn_data_put_int" proto-lib  (_fun _pn_data_t  -> _int)))

(define pn-data-exit (get-ffi-obj "pn_data_exit" proto-lib  (_fun _pn_data_t -> _int)))

(define pn-event-connection (get-ffi-obj "pn_event_connection" proto-lib  (_fun _pn_data_t -> _int)))

(define pn-event-type (get-ffi-obj "pn_event_type" proto-lib  (_fun _pn_event_t -> pn_event_type_t)))

(define pn-session (get-ffi-obj "pn_session" proto-lib  (_fun _pn_data_t -> _int)))

(define pn-connection-set-container (get-ffi-obj "pn_connection_set_container" proto-lib  (_fun _pn_data_t -> _int)))

(define pn-connection-open (get-ffi-obj "pn_connection_open" proto-lib  (_fun _pn_data_t -> _int)))

(define pn-session-open  (get-ffi-obj "pn_session_open" proto-lib  (_fun _pn_data_t -> _int)))

(define pn-sender (get-ffi-obj "pn_sender" proto-lib  (_fun _pn_data_t -> _int)))

(define pn-terminus-set-address (get-ffi-obj "pn_terminus_set_address" proto-lib  (_fun _pn_data_t -> _int)))

(define pn-link-open (get-ffi-obj "pn_link_open" proto-lib  (_fun _pn_data_t -> _int)))

(define pn-link-target (get-ffi-obj "pn_link_target" proto-lib  (_fun _pn_data_t -> _int)))

(define (convert-address host port)
   (let ([p-host (string->bytes/utf-8 host)]
         [p-port (string->bytes/utf-8 port)]
         [buffer (malloc 'atomic-interior 1000)])
     (build-proactor-address buffer 1000 p-host p-port)
     buffer))



; #_(struct proton (host  port  pointer-address [proactor #:auto])
;     #:auto-value
;     #:guard  (λ (host port pointer-address name)(values host port (convert-address host port))))

(struct proton (id address proactor))

(struct connection (host port state)
  #:guard  (λ (host port id  pn_address name)
              (values host port (proton (uuid-generate) (convert-address host port) (pn-proactor)))))



(struct ampq-message (body [id #:mutable]  pn_data_t [pn_message #:auto])
  #:auto-value (pn-message)
   #:guard (λ (body id name) (values body (uuid-generate  (pn-data)))))

(define (make-message-sender conn #:incremental-id [id #f])
  (let ([pn_data (pn-data)]
        [pn_message (pn-message)])
   (if id
    (let ([counter (box 1)])
      (λ (message)
         (pn-message-clear pn-message)
         (let ([body (pn-message-body pn_message)]
               [the-id (unbox counter)])
           (pn-data-put-int (pn-message-id pn_message) the-id)
           (pn-data-put-map  body)
           (pn-data-enter    body)
           (pn-data-put-string body (pn-bytes 6) "message")
           (pn-data-put-string body message)
           (pn-data-exit body))))
    (begin
      (λ (message)
         (pn-message-clear pn-message)
         (let ([body (pn-message-body pn_message)]
               [uuid (uuid-generate)])
           (pn-data-put-string (pn-message-id pn_message) uuid)
           (pn-data-put-map  body)
           (pn-data-enter    body)
           (pn-data-put-string body (pn-bytes 6) "message")
           (pn-data-put-string body message)
           (pn-data-exit body)))))))



(define pn-proactor-connect2 (get-ffi-obj "pn_proactor_connect2" proto-lib (_fun _cpointer _cpointer _cpointer _cpointer -> _void)))

(define pn-proactor (get-ffi-obj "pn_proactor" proto-lib (_fun  ->  _cpointer)))

(define  pn-proactor-wait (get-ffi-obj "pn_proactor_wait" proto-lib (_fun _cpointer -> _cpointer)))


(define (connect host port) (connection host port  ""))

#|(define (event-loop proactor)
  (let banch-loop
    ([events (pn-proactor-wait proactor)])
   (let  next-banch ([event   ( pn-event-banch-next events)]))))
|#



;static bool handle(app_data_t* app, pn_event_t* event) {
; switch (pn_event_type(event)) {
;  case PN_CONNECTION_INIT: {
;    pn_connection_t* c = pn_event_connection(event);
;    pn_session_t* s = pn_session(pn_event_connection(event));
;    pn_connection_set_container(c, app->container_id);
;    pn_connection_open(c);
;    pn_session_open(s);
;    {
;    pn_link_t* l = pn_sender(s, "my_sender");
;    pn_terminus_set_address(pn_link_target(l), app->amqp_address);
;    pn_link_open(l);
;    break;
;    }
;  }
;
(define (event-handler connection event)
  (match (pn-event-type event)
    [ PN_CONNECTION_INIT
      (let* ([conn (pn-event-connection event)]
             [pn-session (pn-session conn)])
         (pn-connection-set-container conn (proton-id (connection-state connection)))
         (pn-connection-open conn)
         (pn-session-open pn-session)
         (let*  ( [pn-link  (pn-sender pn-session "my_sender")])
          (pn-terminus-set-address (pn-link-target pn-link)  (proton-address (connection-state connection)));   ])))
          (pn-link-open pn-link)))]))


; pn_link_t* l = pn_sender(s, "my_sender");
     ; pn_terminus_set_address(pn_link_target(l), app->amqp_address);
     ; pn_link_open(l)]));

; (define (handler-event event)
;   (match (pn-event-connection event)
;      []))
;
;
;
; > (match '(1 2 3)
;     [(list a b a) (list a b)]
;     [(list a b c) (list c b a)])
; '(3 2 1)
;
; > (match '(1 (x y z) 1)
;     [(list a b a) (list a b)]
;     [(list a b c) (list c b a)])
; '(1 (x y z))
;
; > (match #f
;     [else
;      (cond
;        [#f 'not-evaluated]
;        [else 'also-not-evaluated])])
;
; (define _pn_proactor_t (_cpointer 'pn_proactor_t))
;
; (define _pn_event_batch_t (_cpointer '_pn_event_batch_t))
;

; pn_proactor_wait)


; (struct proton-connection (host  port  pn_address [proactor #:auto])
;     #:auto-value (pn-proactor)
;     #:guard  (λ (host port pn_address name)
;                 (values host port (convert-address host port))))


  ; (with-handlers
  ;   ([exn:fail? (λ (exn) (displayln "\nCannot get Data"))])))
