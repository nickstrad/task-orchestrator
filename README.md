# task-orchestrator

An experimental Go orchestrator, built in the spirit of [Kubernetes](https://kubernetes.io/), [Nomad](https://www.nomadproject.io/), and [Apache Mesos](https://mesos.apache.org/).

## Purpose

This is a learning project. The goal is to understand — by building one from scratch — how a distributed orchestrator schedules, runs, and supervises workloads across a cluster.

The broader motivation is to learn what it takes to create **background agent platforms** like [Daytona](https://www.daytona.io/) and [E2B](https://e2b.dev/), which lean heavily on the same primitives: scheduling work onto a pool of compute, isolating and running it, tracking its lifecycle, and recovering from failure.

## Scope

This is not intended to be production-ready. It exists to explore the core ideas behind an orchestrator, which may include:

- A control plane that accepts and stores desired state
- A scheduler that assigns tasks to workers
- Worker agents that run tasks and report status
- Health checking, rescheduling, and failure recovery
- A CLI and/or API for submitting and inspecting work

## Status

Early / experimental. Currently a hello-world scaffold.

## Getting started

```sh
go run .
```
