# TurtleQueue Java SDK

This is the repository for the Java Client for Turtlequeue.

# Quickstart

Add the dependency to your
maven reference here:

Create an account and get an API key.

Create a client
Then create a consumer

Create a producer

The consumer will see the messages.

# Philosophy

Batteries included but optional.

Always included:
- high throughput
- no operations needed
-

Essential batteries include:
- receive-loop
- send-loop

Other optional:
- tracing and monitoring
- java iterator impl for readers

Non-goals:
- Change Apache pulsar's behavior. Especially regarding delivery semantics.


# Turtlequeue-admin

There are two packages available:

- turtlequeue-java
This package allows you to send and receive messages.

- turtlequeue-admin-java
This is analogous to the pulsar admin package. This allows you to create and delete topics, and to mark them as closed.



TODO
custom read/write handlers

TODO
add static analysis tools
lombok findbugs
https://errorprone.info/

javadoc plugin generate + publish on github pages or javadoc.io
https://maven.apache.org/plugins/maven-javadoc-plugin/usage.html
https://ucsb-cs56-pconrad.github.io/topics/javadoc_publishing_to_github_pages_from_public_repo/