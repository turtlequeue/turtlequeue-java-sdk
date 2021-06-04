# TurtleQueue Java SDK
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

This is the repository for the Java SDK for Turtlequeue.
The user documentation is at https://turtlequeue.com/docs/client-libraries-java

Below are developer notes.

# Philosophy

Batteries included but optional.
Follow Apache Pulsar unless it does not make sense.

# Turtlequeue-admin

There are two packages available:

- turtlequeue-java
This package allows you to send and receive messages.

- turtlequeue-admin-java
This is analogous to the pulsar admin package. This allows you to create and delete topics, and to mark them as closed.


# Before merging

- use static analysis tools. Lombok, findbugs
- mvn com.github.spotbugs:spotbugs-maven-plugin:4.1.3:spotbugs && mvn com.github.spotbugs:spotbugs-maven-plugin:4.1.3:gui
- use the licence plugin if there are new files: mvn com.mycila:license-maven-plugin:4.0.rc2:format
