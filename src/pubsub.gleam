import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/static_supervisor.{type Supervisor} as supervisor
import gleam/otp/supervision

pub opaque type Message(element) {
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

pub fn start(name: process.Name(Message(e))) {
  actor.new(dict.new())
  |> actor.named(name)
  |> actor.on_message(handle_message)
  |> actor.start()
}

pub fn subscribe(
  pubsub: Subject(Message(e)),
  client: Subject(e),
  channel: String,
) {
  process.send(pubsub, Subscribe(client, channel))
}

pub fn publish(pubsub: Subject(Message(e)), value: e, channel: String) {
  process.send(pubsub, Publish(value, channel))
}

pub fn unsubscribe(
  pubsub: Subject(Message(e)),
  client: Subject(e),
  channel: String,
) {
  process.send(pubsub, Unsubscribe(client, channel))
}

pub fn shutdown(pubsub: Subject(Message(e))) {
  process.send(pubsub, Shutdown)
}

pub fn pubsub_name() -> process.Name(Message(e)) {
  process.new_name("global")
}

pub fn start_supervisor(
  name: process.Name(Message(e)),
) -> actor.StartResult(Supervisor) {
  supervisor.new(supervisor.OneForOne)
  |> supervisor.auto_shutdown(supervisor.AllSignificant)
  |> supervisor.restart_tolerance(intensity: 3, period: 5)
  |> supervisor.add(
    supervision.worker(fn() { start(name) })
    |> supervision.restart(supervision.Transient)
    // This might be a "bad practice", but I'm curious
    |> supervision.significant(True),
    // This too
  )
  |> supervisor.start()
}
