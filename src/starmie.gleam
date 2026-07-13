import gleam/list
import gleam/erlang/process.{type Subject}
import gleam/io
import gleam/otp/actor

pub fn main() -> Nil {
  let assert Ok(broker) =
    actor.new([]) |> actor.on_message(handle_message) |> actor.start
  let subject = broker.data

  let sub_a = process.new_subject()
  let sub_b = process.new_subject()

  process.send(subject, Subscribe(sub_a))
  process.send(subject, Subscribe(sub_b))

  process.send(subject, Publish("Starmie"))

  let res1 = process.receive(sub_a, 10)

  case res1 {
    Ok(msg) -> io.println("Received: " <> msg)
    Error(Nil) -> io.println("Error receiving message.")
  }

  process.send(subject, Shutdown)
}

pub type Message(element) {
  Shutdown
  Subscribe(message: Subject(element))
  Publish(value: element)
}

fn handle_message(
  subscribers: List(Subject(e)),
  message: Message(e),
) -> actor.Next(List(Subject(e)), Message(e)) {
  case message {
    Shutdown -> actor.stop()
    Subscribe(client) -> {
      let new_client = [client, ..subscribers]
      actor.continue(new_client)
    }
    Publish(value) -> {
      list.each(subscribers, fn(subscriber) {
        process.send(subscriber, value)
      })
      actor.continue(subscribers)
    }
  }
}
