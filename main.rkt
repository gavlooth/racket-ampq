#lang racket

(require ffi/unsafe)
(require describe)
(require setup/dirs)



;app.container_id = argv[0];   /* Should be unique */
;app.host = (argc > 1) ? argv[1] : "";
;app.port = (argc > 2) ? argv[2] : "amqp";
;app.amqp_address = (argc > 3) ? argv[3] : "examples";
;app.message_count = (argc > 4) ? atoi(argv[4]) : 10);
;
;#:reflection-name 'ampq
; (struct pipa (a b c)
;   #:guard (λ (a b c name) (values  2 3 4)))
;
; (pipa-a (pipa 'a #f 'f))
;  #:property prop:procedure (struct-field-index base))
;
;
; (define happy+ (mood-procedure add1 10))
(define SEQ (string->bytes/utf-8 "sequence"))

(file-name-from-path (car  (string-split (getenv "LD_LIBRARY_PATH") ":")))

(define (locate-proto-path)
  (let ([paths (map string->path
                  (map (λ (x)
                         (if (string-suffix? x "/") (format "~alibqpid-proton.so" x)
                           (format "~a/libqpid-proton.so" x)))
                       (string-split (getenv "LD_LIBRARY_PATH") ":")))])
    (ormap (λ (x) (if (file-exists? x) x #f) ) paths)))


(define proto-lib  (ffi-lib (locate-proto-path)))

(define build-proactor-address (get-ffi-obj "pn_proactor_addr" proto-lib (_fun _pointer _int _pointer _pointer  ->  _int)))


(define _pn_message_t (_cpointer 'pn_message_t))


(define _pn_data_t  (_cpointer 'pn_data_t))

(define _pn_message (_cpointer 'pn_message))

(define _pn_bytes_t (_cpointer 'pn_bytes_t))

(define  pn_session_t (_cpointer 'pn_session_t))
(define pn_connection_t  (_cpointer 'pn_connection_t))

(define pn-message (get-ffi-obj "pn_message_clear" proto-lib (_fun _void -> _pn_message)))

(define pn-data-enter (get-ffi-obj "pn_data_enter" proto-lib (_fun _pn_data_t -> _bool)))

(define pn-message-id (get-ffi-obj "pn_message_id" proto-lib (_fun _pn_message_t -> _pn_data_t)))

(define pn-message-clear  (get-ffi-obj "pn_message_clear" proto-lib  (_fun _pn_message_t -> _void)))

(define pn-message-body  (get-ffi-obj "pn_message_clear" proto-lib  (_fun _pn_message_t -> _pn_data_t)))

(define pn-data-put-map (get-ffi-obj "pn_data_put_map" proto-lib  (_fun _pn_data_t -> _int)))

(define pn-bytes (get-ffi-obj "pn_bytes" proto-lib  (_fun _int _string -> _pn_bytes_t)))

(define pn-data-put-string(get-ffi-obj " pn_data_put_string" proto-lib  (_fun _pn_data_t _string  -> _int)))

(define pn-data-put-int (get-ffi-obj "pn_data_put_int" proto-lib  (_fun _pn_data_t  -> _int)))

(define pn-data-exit (get-ffi-obj "pn_data_exit " proto-lib  (_fun _pn_data_t -> _int)))

(define pn-event-connection (get-ffi-obj "pn_event_connection" proto-lib  (_fun _pn_data_t -> _int)))

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

; (bytes-length SEQ)
; (cast SEQ _cpointer _string)
(define (struct->message sc)
  (define message (pn-message))
  (define body (pn-message-body message))

  (pn-data-put-map  body);
  (pn-data-ender body);
  (pn-data-put-string body (pn-bytes 7 "sequence")))

; pn_data_put_string(body, pn_bytes(sizeof("sequence")-1, "sequence"));
(define pn-proactor-connect2 (get-ffi-obj "pn_proactor_connect2" proto-lib (_fun _pointer _pointer _pointer _pointer -> _void)))

(define pn-proactor (get-ffi-obj "pn_proactor" proto-lib (_fun  ->  _pointer)))

(struct proton (host  port  pointer-address [proactor #:auto])
  #:auto-value (pn-proactor)
  #:guard  (λ (host port pointer-address name)(values host port (convert-address host port))))


; (struct proton-2 (host  port  [address #:auto #:mutable] [proactor #:auto])
;   #:auto-value (pn-proactor)
;   #:guard  (λ (host port name)(values host port  (convert-address host port))))


(define (->proton host port) (proton host port ""))

(define x (->proton  "localhost" "ampq"))


(define (->connect pr)
  (pn-proactor-connect2 (proton-proactor pr)  #f #f (proton-pointer-address pr)))

(define y (->connect x))


; pn_data_put_map(body);
;  pn_data_enter(body));
pn_data_put_string(body, pn_bytes(sizeof("sequence")-1, "sequence"));
 pn_data_put_int(body, app->sent); /* The sequence number */
 pn_data_exit(body);


;void pn_message_clear      	(      	pn_message_t *       	msg)



; (define pn-proactor-connect2
;   (get-ffi-obj
;     "pn_proactor_connect2"
;     proto-lib (_fun _pointer _pointer
;                     _pointer _pointer ->
;                     _void)))
#|

 /* Create a message with a map { "sequence" : number } encode it and return the encoded buffer. */
static void send_message(app_data_t* app, pn_link_t *sender) {
  /* Construct a message with the map { "sequence": app.sent } */
  pn_data_t* body;
  pn_message_clear(app->message);
  body = pn_message_body(app->message);
  pn_data_put_int(pn_message_id(app->message), app->sent); /* Set the message_id also */
  pn_data_put_map(body);
  pn_data_enter(body);
  pn_data_put_string(body, pn_bytes(sizeof("sequence")-1, "sequence"));
  pn_data_put_int(body, app->sent); /* The sequence number */
  pn_data_exit(body);
  if (pn_message_send(app->message, sender, &app->message_buffer) < 0) {
    fprintf(stderr, "error sending message: %s\n", pn_error_text(pn_message_error(app->message)));
    exit(1);
  }
}
|#
#|(connect->proton-proactor   )
|#
;(define SIZE 1000)
;(define buffer (malloc 'atomic SIZE))
;(memset buffer 0 SIZE)
;(define pn-connection-pointer (_cpointer "pn_connection_t"))
;(define pn-connection (get-ffi-obj "pn_connection" proto-lib (_fun -> pn-connection-pointer)))
; (define pn-connection (get-ffi-obj "pn_proactorjk" proto-lib (_fun -> pn-connection-pointer)))


#|(define pn-connection-state (get-ffi-obj "pn_connection_state" proto-lib (_fun pn-connection-pointer -> _int)))
(describe pn-connection-state)
(define connect (pn-connection))
(pn-connection-state connect)
(define pipa (pn-connection))
|#
#|(define (locate-proto-path-2)
   (let ([paths (map string->path
                   (map (λ (x)
                          (if (string-suffix? x "/") (format "~alibqpid-proton-core.so" x)
                            (format "~a/libqpid-proton-core.so" x)))
                        (string-split (getenv "LD_LIBRARY_PATH") ":")))])
     (ormap (λ (x) (if (file-exists? x) x #f) ) paths)))

|#
