package provide tanzer::session 0.1

##
# @file tanzer/session.tcl
#
# The HTTP session handler
#

package require tanzer::message::chunk
package require tanzer::response
package require tanzer::error
package require TclOO

##
# Default values for ::tanzer::session object state.
#
namespace eval ::tanzer::session {
    variable timeout 5
}

##
# The HTTP session handler class.  For each new inbound connection accepted, a
# new session handler is created to handle requests for the server object
# `$newServer`, referencing channel `$newSock`, and request protocol in
# `$newProto`.
#
::oo::class create ::tanzer::session

##
# Create a new session handler to service an incoming request from server
# `$newServer`, inbound from channel `$newSock`, in the request protocol
# specified in `$newProto`.
#
::oo::define ::tanzer::session constructor {newServer newSock newProto} {
    my variable server sock proto request route handler cleanup \
        response responded buffer config remaining keepalive watchdog \
        store requestBodyFinished

    set server    $newServer
    set sock      $newSock
    set proto     $newProto
    set request   {}
    set requestBodyFinished 0
    set route     {}
    set handler   {}
    set cleanup   [list]
    set response  {}
    set responded 0
    set buffer    ""
    set remaining 0
    set keepalive 1
    set watchdog  {}
    set store [dict create]

    set config(readsize) [$newServer config readsize]

    my monitor
}

::oo::define ::tanzer::session destructor {
    my variable server sock request response \
        watchdog cleanup

    if {$request ne {}} {
        $request destroy
    }

    if {$response ne {}} {
        $response destroy
    }

    $server forget $sock

    catch {
        close $sock
    }

    my monitor -cancel
    my cleanup
}

##
# Install the watchdog timer for the current session.
#
::oo::define ::tanzer::session method monitor {args} {
    my variable watchdog

    array set opts {
        cancel 0
    }

    foreach arg $args {
        switch -- $arg "-cancel" {
            set opts(cancel) 1
        } default {
            error "Invalid flag $arg"
        }
    }

    if {$opts(cancel)} {
        if {$watchdog ne {}} {
            after cancel $watchdog
            set watchdog {}
        }

        return
    }

    set watchdog [after [expr {$::tanzer::session::timeout * 1000}] \
        [self] destroy]
}

##
# Report the error `$e` to the server, and end the current session, resulting
# in the destruction of the current object.
#
::oo::define ::tanzer::session method error {e} {
    my variable server sock

    $server error $e $sock
}

##
# When `$args` is not empty, specify a callback to be dispatched whenever the
# current session handler is ready to clean any associated state and prepare
# to handle a new request, or to end.  Otherwise, when no arguments are
# specified, any previously provided cleanup callbacks are executed precisely
# once, in the order in which they were registered, and are subsequently
# cleared.
#
# Each successive call to this method results in another cleanup task queued to
# be executed upon session termination.
#
# If `-reset` is specified in the first argument, then all cleanup tasks are
# cleared for the current session.
#
::oo::define ::tanzer::session method cleanup {args} {
    my variable cleanup

    if {[llength $args] > 0} {
        if {[lindex $args 0] eq "-reset"} {
            set cleanup [list]
        } else {
            lappend cleanup $args
        }

        return
    }

    foreach script $cleanup {
        {*}$script
    }

    set cleanup [list]
}

##
# Delegate events to a different request handler as specified in `$args`.
#
::oo::define ::tanzer::session method delegate {args} {
    my variable handler

    return [set handler $args]
}

::oo::define ::tanzer::session method ready {} {
    my variable sock keepalive

    return [expr {$keepalive && ![eof $sock]}]
}

##
# Clear any state associated with the session's current request, and prepare
# the session handler to handle a new request.
#
::oo::define ::tanzer::session method nextRequest {} {
    my variable server sock buffer handler route \
        request response responded remaining keepalive \
        store

    if {$request ne {}} {
        $request destroy
        set request {}
    }

    if {$response ne {}} {
        $response destroy
        set response {}
    }

    my cleanup

    set route     {}
    set handler   {}
    set store [dict create]
    set responded 0

    if {$remaining != 0} {
        ::tanzer::error throw 400 "Invalid request body length"
    }

    #
    # If the session shant be kept alive, then end it.
    #
    if {!$keepalive} {
        my destroy

        return
    }

    my reset read

    if {![my ready]} {
        return
    }

    #
    # If we still have data in the buffer, then we'll need to flush that out.
    #
    if {[string length $buffer] > 0} {
        my handle read
    }

    #
    # Finally, reenable the watchdog timer for the current session.
    #
    my monitor
}

##
# Handle the `read` or `write` event as specified in `$event`.  Upon the
# receipt of `read` events, continue to buffer and retain request data until
# enough data is present to parse a full request message, route the request to
# the appropriate request handler, and delegate all subsequent `write` events
# to the new handler.
# 
# Event handlers are called with the following form when dispatching `read`
# events:
#
# @code{.tcl}
# {*}$handler $event $session $data
# @endcode
#
# Event handlers are called with the following form when dispatching `write`
# events:
#
# @code{.tcl}
# {*}$handler $event $session
# @endcode
#
::oo::define ::tanzer::session method handle {event} {
    my variable sock server request keepalive \
        buffer config handler watchdog remaining \
        requestBodyFinished

    if {$event eq "write"} {
        return [{*}$handler write [self]]
    }

    if {$event ne "read"} {
        error "Unknown event '$event'"
    }

    #
    # Given that this very well could be our first opportunity to read data,
    # let's make no assumptions about the state of the request and read some
    # data and pass it off to the request to see if the data is complete enough
    # to allow it to parse the headers, at least.
    #
    set data [read $sock $config(readsize)]

    #
    # Create a request if one does not exist already.
    #
    if {$request eq {}} {
        set request [my request -new]
    }

    #
    # Is the request complete yet?
    #
    if {[$request incomplete]} {
        #
        # If not, buffer the data to the request and attempt to parse it.
        #
        append buffer $data

        #
        # Bail if the request is not yet parseable.
        #
        if {![$request parse buffer]} {
            return
        }

        my monitor -cancel

        #
        # If the request does not call for keepalive, then set the keepalive
        # count to 0.
        #
        if {![$request keepalive]} {
            set keepalive 0
        }

        #
        # We are now ready to attempt to route a handler to the
        # request.
        #
        my route

        #
        # Indicate the amount of bytes left to be read by the session handler.
        #
        set remaining [$request length]

        #
        # Flush out the existing data left over from the session buffer
        # to the handler as a read event, and subsequently trim the buffer
        # to only the parts we need.
        #
        set start 0
        set end   [expr {$remaining - 1}]
        set data  [string range $buffer $start $end]
    } else {
        append buffer $data
    }

    #
    # Pass the current block of data to the handler.
    #
    if {[$request chunked]} {
        #
        # If there is no more chunk data to decode, then we can indicate that
        # we are now ready to transition to write-ready events.
        #
        if {![::tanzer::message::chunk parse buffer {*}$handler read [self]]} {
            set requestBodyFinished 1
        }
    } else {
        #
        # Decrement the length of the data passed to the request handler from
        # the number of bytes left for this current request.
        #
        incr remaining -[string length $data]

        #
        # If there is no more body to be read from the request body, then we
        # can transition to write-ready events.
        #
        if {$remaining == 0} {
            set requestBodyFinished 1
        }

        {*}$handler read [self] $data
    }

    #
    # If the request is now ready, then bind the event handler to the socket
    # for writable events, and ignore readable events.
    #
    # Now, before I forget my post-rationalization, precisely, why would we
    # want to do this?  The answer is simple, my love.  This affords the
    # request handler the autonomy to decide whether or not it wants to read
    # from the socket at its own whim.  And...AND, it ought to be up to the
    # request handler to determine when it's ready to cede control back to
    # the session and spawn up a new request handler, at least in the case
    # of HTTP.
    #
    if {$requestBodyFinished} {
        chan event $sock readable {}
        chan event $sock writable [list $server respond write $sock]
    }

    return
}

::oo::define ::tanzer::session method requestBodyFinished {} {
    my variable requestBodyFinished

    return $requestBodyFinished
}

##
# Return a reference to the server for which the current session handler is
# open.
#
::oo::define ::tanzer::session method server {} {
    my variable server

    return $server
}

##
# Return a reference to the socket channel the current request handler is
# reading from and writing to.
#
::oo::define ::tanzer::session method sock {} {
    my variable sock

    return $sock
}

##
# When no arguments are specified in `$args`, return a reference to the most
# recent request parsed by the current session handler.  Otherwise, if the flag
# `-new` is specified, then create a new request object to parse an inbound
# request of the protocol for which this session handler is servicing, save the
# reference to the new request object, and return that.
#
::oo::define ::tanzer::session method request {args} {
    my variable sock proto request keepalive

    array set opts {
        new 0
    }

    if {[llength $args] == 0} {
        return $request
    }

    foreach arg $args {
        switch -- $arg "-new" {
            set opts(new) 1
        } default {
            error "Invalid argument $arg"
        }
    }

    if {$opts(new)} {
        set module [format "::tanzer::%s::request" $proto]

        set request [$module new [self]]
        set server  [chan configure $sock -sockname]
        set peer    [chan configure $sock -peername]

        $request env SERVER_ADDR [lindex $server 0]
        $request env SERVER_HOST [lindex $server 1]
        $request env SERVER_PORT [lindex $server 2]
        $request env REMOTE_ADDR [lindex $peer   0]
        $request env REMOTE_HOST [lindex $peer   1]
        $request env REMOTE_PORT [lindex $peer   2]
    }

    return $request
}

##
# Return the value of configuration variable `$name` as set at object
# instantiation time.  If `$value` is also provided, then set or replace the
# configuration option `$name` with `$value`.  Otherwise, return a list of
# key-value pairs containing all current configuration data.
#
::oo::define ::tanzer::session method config {args} {
    my variable config

    switch -- [llength $args] 0 {
        return [array get config]
    } 1 {
        lassign $args name

        return $config($name)
    } 2 {
        lassign $args name value

        return [set $config($name) $value]
    }

    error "Invalid command invocation"
}

##
# Read a block of data from the current session's socket.  The read buffer
# size is the `readsize` configuration option in ::tanzer::server.
#
::oo::define ::tanzer::session method read {} {
    my variable config sock 

    return [read $sock $config(readsize)]
}

##
# Write `$data` to the current session's socket, and return the number of
# bytes written.  If the session has a response object already prepared, and
# that response contains a `Transfer-Encoding:` header which indicates the
# `chunked` encoding, then `$data` will be written to the remote end in a
# chunked transfer encoded fragment.
#
::oo::define ::tanzer::session method write {data} {
    my variable sock response

    set length [string length $data]

    if {[$response chunked]} {
        puts -nonewline $sock [format "%x\r\n$data\r\n" $length]
    } else {
        puts -nonewline $sock $data
    }

    return $length
}

##
# Using `[chan event]` on the current session's socket, bind all events of the
# type specified in `$eventType` to the command specified in `$args`.
#
::oo::define ::tanzer::session method bind {eventType args} {
    my variable sock

    chan event $sock $eventType $args
}

##
# Reset the socket attached to the current session to monitor for the event
# type specified in `$event` as per the following:
#
# * `read`
#
# * `write`
#
# * `any`
#
#   Implies both `read` and `write`
#
# * `none`
#
#   Implies neither `read` nor `write`
# .
#
# I/O readiness events will be dispatched by the server once again, which shall
# then dispatch the handler specified in the last call to the method
# ::tanzer::session::delegate.  If either `read` or `write` is not specified
# explicitly or implicitly by `any`, then I/O event response is canceled for
# that event type.
#
# `$event` defaults to `any` if it is not specified.
#
::oo::define ::tanzer::session method reset {{event "any"}} {
    my variable sock server

    array set eventTypes {
        read  readable
        write writable
        any   {readable writable}
        none  {}
    }

    if {[array get eventTypes $event] eq {}} {
        error "Invalid event $event"
    }

    chan event $sock readable {}
    chan event $sock writable {}

    foreach eventType $eventTypes($event) {
        chan event $sock $eventType [list $server respond $event $sock]
    }

    return
}

##
# Send the response object in `$newResponse` to the client.  If the session
# handler has determined that the session can and should be kept alive, then a
# `Connection:` header is set with the `Keep-Alive` value, otherwise `Close`.
# Then, all headers are sent down the wire, and any response data buffered in
# `$newResponse` is sent as well.
#
# After sending the response, `$newResponse` is kept by the session handler for
# future reference by callers, and log the current session's request and new
# response with the server.
#
::oo::define ::tanzer::session method send {newResponse} {
    my variable server sock request response responded keepalive

    if {$responded} {
        error "Already sent response"
    }

    if {![$newResponse keepalive]} {
        set keepalive 0
    }

    $server log $request $newResponse

    $newResponse header Connection [expr {$keepalive? "Keep-Alive": "Close"}]
    $newResponse send $sock

    set response  $newResponse
    set responded 1

    return
}

##
# If a response was previously queued by a call to ::tanzer::session::response,
# then send that response using ::tanzer::session::send.
#
::oo::define ::tanzer::session method respond {} {
    my variable response

    if {$response eq {}} {
        error "No response queued"
    }

    my send $response
}

##
# Returns true if a response has been sent for the last request handled by this
# session.
#
::oo::define ::tanzer::session method responded {} {
    my variable responded

    return $responded
}

##
# This method yields different behaviors when called in any of the following
# ways.
#
# * `[$session response]`
#
#   Return the last response recorded for this session, if any.
#
# * `[$session response -new $response]`
#
#   Record the new response provided in `$response` in the current session.
#
# * `[$session response $args]`
#
#   Call the response object command last recorded for the current session,
#   passing the arguments in `$args` in expanded form with `{*}` prepended.
#   The result of this response command invocation is returned.
# .
#
::oo::define ::tanzer::session method response {args} {
    my variable response responded

    if {[llength $args] == 0} {
        return $response
    }

    if {$responded} {
        error "Already sent response"
    }

    if {[lindex $args 0] eq "-new"} {
        #
        # Destroy any existing response object there may be, if it has not
        # been served yet.
        #
        if {$response ne {}} {
            $response destroy
        }

        if {[llength $args] != 2} {
            error "Invalid command invocation"
        }

        return [set response [lindex $args 1]]
    }

    if {$response eq {}} {
        error "No response recorded"
    }

    return [uplevel 1 [list $response {*}$args]]
}

##
# Send a 301 Redirect to the client, referring them to the new location `$uri`.
#
::oo::define ::tanzer::session method redirect {uri} {
    my variable response

    set response [::tanzer::response new 301]

    $response header Location       $uri
    $response header Content-Length 0

    my send $response
}

##
# Returns true if no data has been parsed for the current request, and if the
# remote socket has reached end-of-file status.
#
::oo::define ::tanzer::session method ended {} {
    my variable sock request

    return [expr {[eof $sock] && [$request empty]}]
}

##
# Return true if the current session is to be kept alive.  If a boolean
# argument is provided, the keepalive setting is set to its value.
#
::oo::define ::tanzer::session method keepalive {args} {
    my variable keepalive

    switch -- [llength $args] 0 {
        return $keepalive
    } 1 {
        return [lassign $args kepalive]
    }

    error "Invalid command invocation"
}

##
# Search through the server's route table in order for the first route that
# matches the current request, make note of that route, and return the route
# to the caller.  If a route has already been selected for the current request,
# then return that immediately.  Otherwise, if no suitable route can be matched
# to the current request, then throw a 404 error.
#
::oo::define ::tanzer::session method route {} {
    my variable server request route handler

    if {$route ne {}} {
        return $route
    }

    set router  [$server router]
    set route   [$router route $request]
    set handler [$route script]

    return $route
}

##
# Return the value of the stored session variable `$name`
# If `$value` is also provided, then set or replace the
# session variable `$name` with `$value`.  Otherwise, return a list of
# key-value pairs containing all current session variables.
# #
::oo::define ::tanzer::session method store {args} {
    my variable store

    switch -- [llength $args] 0 {
        return $store
    } 1 {
        set key [lindex $args 0]
        if {[dict exists $store $key]} {
            return [dict get $store $key]
        }
        return {}
    } 2 {
        dict set store [lindex $args 0] [lindex $args 1]
        return
    }

    error "Invalid command invocation"
}
