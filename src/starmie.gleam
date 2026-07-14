import gleam/io
import gleam/erlang/process
import pubsub.{subscribe, unsubscribe, publish, shutdown, start}

pub fn main() -> Nil {
  let assert Ok(broker) = start()
  let subject = broker.data

  let sub_a = process.new_subject()
  let sub_b = process.new_subject()

  subscribe(subject, sub_a, "pokemon")
  subscribe(subject, sub_b, "digimon")

  publish(subject, "Starmie", "pokemon")

  case process.receive(sub_a, 10) {
    Ok(msg) -> io.println("sub_a received: " <> msg)
    Error(Nil) -> io.println("ERROR: sub_a missed the message")
  }

  case process.receive(sub_b, 10) {
    Ok(msg) -> io.println("ERROR: sub_b intercepted: " <> msg)
    Error(Nil) -> io.println("sub_b ignored 'pokemon' channel")
  }

  publish(subject, "Agumon", "digimon")

  case process.receive(sub_b, 10) {
    Ok(msg) -> io.println("sub_b received: " <> msg)
    Error(Nil) -> io.println("ERROR: sub_b missed the message")
  }

  unsubscribe(subject, sub_a, "pokemon")

  case process.receive(sub_a, 10) {
    Ok(msg) -> io.println("ERROR: sub_a still receive: " <> msg)
    Error(Nil) -> io.println("sub_a successfully unsubscribe")
  }

  publish(subject, "Staryu", "pokemon")



  shutdown(subject)
}
