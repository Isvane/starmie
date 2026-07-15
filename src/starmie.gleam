import gleam/erlang/process
import gleam/io
import pubsub

pub fn main() -> Nil {
  // This feels illegal but eh...
  process.trap_exits(True)

  let name = process.new_name("global")

  let assert Ok(_) = pubsub.start_supervisor(name)

  let subject = process.named_subject(name)

  let sub_a = process.new_subject()
  let sub_b = process.new_subject()

  pubsub.subscribe(subject, sub_a, "pokemon")
  pubsub.subscribe(subject, sub_b, "digimon")

  pubsub.publish(subject, "Staryu evolved into Starmie!", "pokemon")
  pubsub.publish(subject, "Koromon digivolve to Agumon!", "digimon")

  let selector =
    process.new_selector()
    |> process.select(sub_a)
    |> process.select(sub_b)

  case process.selector_receive(selector, 50) {
    Ok(msg) -> io.println(msg)
    Error(_) -> io.println("Timed out")
  }

  case process.selector_receive(selector, 50) {
    Ok(msg) -> io.println(msg)
    Error(_) -> io.println("Timed out")
  }

  pubsub.shutdown(subject)
  process.sleep(50)
}
