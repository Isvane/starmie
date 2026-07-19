# 🌟 Starmie

Starmie is a lightweight, type-safe Publish-Subscribe (PubSub) actor pattern implementation written in Gleam. It bridges Erlang's OTP actor system with a channel-based broadcasting model, complete with process health tracking and automated supervisor setups. Built as my first ever Gleam project.

---

## Running the Code

Since this is a standalone learning project, you can run the validation suite directly using the Gleam CLI.
```console
gleam run
```

---

## Glimpse

Here’s the whole setup in action. We spin up two subscribers, lock them into separate fandom channels, and verify that the routing works exactly as it should.
```gleam
import gleam/erlang/process
import gleam/io
import pubsub

pub fn main() -> Nil {
  process.trap_exits(True)

  let name = process.new_name("global")
  let assert Ok(_) = pubsub.start_supervisor(name)

  let subject = process.named_subject(name)
  let sub_a = process.new_subject()
  let sub_b = process.new_subject()

  // Hook up the subscribers to their respective channels
  pubsub.subscribe(subject, sub_a, "pokemon")
  pubsub.subscribe(subject, sub_b, "digimon")

  // Combine both subscriber subjects into a single, unified Selector
  let selector =
    process.new_selector()
    |> process.select(sub_a)
    |> process.select(sub_b)

  // Drop a message into the pokemon channel
  pubsub.publish(subject, "Staryu evolved into Starmie!", "pokemon")

  // Wait concurrently on both subjects. The first arriving message unblocks us immediately
  case process.selector_receive(selector, 50) {
    Ok(msg) -> io.println(msg)
    Error(Nil) -> io.println("Timed out")
  }

  pubsub.shutdown(subject)
  process.sleep(50)
}
```
