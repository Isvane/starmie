import gleam/erlang/process.{type Subject}
import gleam/io
import gleam/otp/actor

pub fn main() -> Nil {
  let assert Ok(actor) =
    actor.new([]) |> actor.on_message(handle_message) |> actor.start
  let subject = actor.data

  process.send(subject, Push("Starmie"))
  process.send(subject, Push("Staryu"))

  let res1 = process.call(subject, 10, Pop)

  case res1 {
    Ok(pokemon) -> io.println("Popped: " <> pokemon)
    Error(Nil) -> io.println("Stack is empty")
  }

  let res2 = process.call(subject, 10, Pop)

  case res2 {
    Ok(pokemon) -> io.println("Popped: " <> pokemon)
    Error(Nil) -> io.println("Stack is empty")
  }

  let assert Error(Nil) = process.call(subject, 10, Pop)

  process.send(subject, Shutdown)
}

pub type Message(element) {
  Shutdown
  Push(push: element)
  Pop(reply_with: Subject(Result(element, Nil)))
}

fn handle_message(
  stack: List(e),
  message: Message(e),
) -> actor.Next(List(e), Message(e)) {
  case message {
    Shutdown -> actor.stop()

    Push(value) -> {
      let state = [value, ..stack]
      actor.continue(state)
    }

    Pop(client) -> {
      case stack {
        [] -> {
          process.send(client, Error(Nil))
          actor.continue([])
        }

        [first, ..rest] -> {
          process.send(client, Ok(first))
          actor.continue(rest)
        }
      }
    }
  }
}
