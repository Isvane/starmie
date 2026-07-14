import gleam/erlang/process
import gleam/io
import pubsub

pub fn main() -> Nil {
  let name = process.new_name("global")

  let assert Ok(_) = pubsub.start_supervisor(name)

  let subject = process.named_subject(name)

  let sub_a = process.new_subject()
  let sub_b = process.new_subject()

  pubsub.subscribe(subject, sub_a, "pokemon")
  pubsub.subscribe(subject, sub_b, "digimon")

  pubsub.publish(subject, "Staryu evolved into Starmie!", "pokemon")

  case process.receive(sub_a, 10) {
    Ok(msg) -> io.println("SUCCESS: sub_a received: " <> msg)
    Error(Nil) -> io.println("ERROR: sub_a missed the message")
  }

  case process.receive(sub_b, 10) {
    Ok(msg) -> io.println("ERROR: sub_b intercepted: " <> msg)
    Error(Nil) -> io.println("SUCCESS: sub_b ignored 'pokemon' channel!")
  }

  pubsub.publish(subject, "Koromon digivolve to Agumon!", "digimon")

  case process.receive(sub_b, 10) {
    Ok(msg) -> io.println("SUCCESS: sub_b received: " <> msg)
    Error(Nil) -> io.println("ERROR: sub_b missed the message")
  }

  case process.receive(sub_a, 10) {
    Ok(msg) -> io.println("ERROR: sub_a intercepted: " <> msg)
    Error(Nil) -> io.println("SUCCESS: sub_a ignored 'digimon' channel!")
  }
}
