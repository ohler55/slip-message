# slip-message

A message package for [Slip](https://github.com/ohler55/slip) that
supports [NATS](https://nats.io) messaging as well as an in memory
messaging implementation.

## Building

The package is implemented as a Go plugin. All plugins require that
the version of the code pulling in the plugin and the plugin version
match. That that means is the plugin must be build with the same
version of SLIP in order to make use of the `require` LISP function to
load the message package. It's actually a bit more finicky than that
though. The build must be done against the actual source code and not
simply putting the version in the go.mod requires.

## Getting Started

 1. Install
 2. Run
 3. Explore

### Install

Slip-message is a plugin for the
[Slip](https://github.com/ohler55/slip) Lisp environment. The
slip-message plugin can be imported with the Lisp `require` function
or as an alternative the [slap](https://github.com/ohler55/slap)
application can be built. The slap application is a standalone version
of Slip with the what ever plugins desired already imported making for
a simplier way to get the environment up and running.

To make the slap application with the slip-message plugin included,
checkout the [slap](https://github.com/ohler55/slap) repository and
build from the master branch by typing:

```
> make
```

The slap applicaiton in the top level directory ready to be used or
copied your choice of a `bin` directory.

### Run

Just run the slap application.

```
> slap
```

The Slip REPL will start and be ready for commands.

### Explore

A good way to explore the features of slip-message once in the slap
REPL is to use the `apropos` and `describe` function.

```lisp
▶ (apropos 'message)

```

The result will include functions such as:

```
message:*message* = #<package message>
message:message-hub-close (built-in)
message:message-publish (built-in)
message:message-request (built-in)
message:message-subscribe (built-in)
message:message-subscribers (built-in)
message:message-unsubscribe (built-in)
```

To see more details try describing the package or the functions. Note
the package description includes a trivial example.

```lisp
▶ (describe *message*)
```

Another way to see the description of a function is to start typing
the function as if it was being called then tab to complete followed
by option-?.

```lisp
▶ (make-app-hub
```
