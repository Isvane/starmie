import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor

pub fn main() -> Nil {
  let assert Ok(broker) =
    actor.new(dict.new()) |> actor.on_message(handle_message) |> actor.start
  let subject = broker.data

  let sub_a = process.new_subject()
  let sub_b = process.new_subject()

  process.send(subject, Subscribe(sub_a, "pokemon"))
  process.send(subject, Subscribe(sub_b, "digimon"))

  process.send(subject, Publish("Starmie", "pokemon"))

  case process.receive(sub_a, 10) {
    Ok(msg) -> io.println("sub_a received: " <> msg)
    Error(Nil) -> io.println("ERROR: sub_a missed the message")
  }

  case process.receive(sub_b, 10) {
    Ok(msg) -> io.println("ERROR: sub_b intercepted: " <> msg)
    Error(Nil) -> io.println("sub_b ignored 'pokemon' channels")
  }

  process.send(subject, Publish("Agumon", "digimon"))

  case process.receive(sub_b, 10) {
    Ok(msg) -> io.println("sub_b received: " <> msg)
    Error(Nil) -> io.println("ERROR: sub_b missed the message")
  }

  process.send(subject, Unsubscribe(sub_a, "pokemon"))

  process.send(subject, Publish("Staryu", "pokemon"))

  case process.receive(sub_a, 10) {
    Ok(msg) -> io.println("ERROR: sub_a still receive: " <> msg)
    Error(Nil) -> io.println("sub_a successfully unsubscribe")
  }

  process.send(subject, Shutdown)
}

pub type Message(element) {
  Shutdown
  Subscribe(message: Subject(element), channel: String)
  Unsubscribe(sub: Subject(element), channel: String)
  Publish(value: element, channel: String)
}

fn handle_message(
  subscribers: dict.Dict(String, List(Subject(e))),
  message: Message(e),
) -> actor.Next(dict.Dict(String, List(Subject(e))), Message(e)) {
  case message {
    Shutdown -> actor.stop()

    Subscribe(client, channel) -> {
      let updated_subscribers =
        dict.upsert(subscribers, channel, fn(maybe_list) {
          case maybe_list {
            option.None -> [client]
            option.Some(existing_subs) -> [client, ..existing_subs]
          }
        })
      actor.continue(updated_subscribers)
    }

    Unsubscribe(client, channel) -> {
      let channel_subscribers = dict.get(subscribers, channel)
      case channel_subscribers {
        Ok(sub_list) -> {
          let filtered_subs = list.filter(sub_list, fn(sub) { sub != client })
          let updated_subscribers = case filtered_subs {
            [] -> dict.delete(subscribers, channel)
            _ -> dict.insert(subscribers, channel, filtered_subs)
          }
          actor.continue(updated_subscribers)
        }
        Error(_) -> actor.continue(subscribers)
      }
    }

    Publish(value, channel) -> {
      case dict.get(subscribers, channel) {
        Ok(sub_list) -> {
          let alive =
            list.filter(sub_list, fn(sub) {
              case process.subject_owner(sub) {
                Ok(pid) -> process.is_alive(pid)
                Error(Nil) -> False
              }
            })

          list.each(alive, process.send(_, value))

          case alive {
            [] -> actor.continue(dict.delete(subscribers, channel))
            _ -> actor.continue(dict.insert(subscribers, channel, alive))
          }
        }
        Error(_) -> actor.continue(subscribers)
      }
    }
  }
}
