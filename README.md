# 🌟 Starmie

Starmie is a lightweight, type-safe Publish-Subscribe (PubSub) actor pattern implementation written in Gleam. It bridges Erlang's OTP actor system with a channel-based broadcasting model, complete with process health tracking and automated supervisor setups.

---

## Running the Example

Since this is a standalone learning project, you can run the validation suite directly using the Gleam CLI.
```console
gleam run
```

---

## Explanation

Here’s the whole setup in action. We spin up two subscribers, lock them into separate fandom channels, and verify that the routing works exactly as it should.
```gleam
import gleam/erlang/process
import gleam/io
import pubsub

pub fn main() -> Nil {
  let name = process.new_name("global")
  let assert Ok(_) = pubsub.start_supervisor(name)

  let subject = process.named_subject(name)
  let sub_a = process.new_subject()
  let sub_b = process.new_subject()

  // Hook up the subscribers to their channels
  pubsub.subscribe(subject, sub_a, "pokemon")
  pubsub.subscribe(subject, sub_b, "digimon")

  // Drop a message into the pokemon channel
  pubsub.publish(subject, "Staryu evolved into Starmie!", "pokemon")

  // sub_a catches it!
  case process.receive(sub_a, 10) {
    Ok(msg) -> io.println("SUCCESS: sub_a received: " <> msg)
    Error(Nil) -> io.println("ERROR: sub_a missed the message")
  }

  // sub_b correctly ignores it.
  case process.receive(sub_b, 10) {
    Ok(msg) -> io.println("ERROR: sub_b intercepted: " <> msg)
    Error(Nil) -> io.println("SUCCESS: sub_b ignored 'pokemon' channel!")
  }
}
```
