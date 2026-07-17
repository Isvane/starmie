import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/static_supervisor.{type Supervisor} as supervisor
import gleam/otp/supervision

pub opaque type Message(element) {
  Shutdown
  Subscribe(client: Subject(element), channel: String)
  Unsubscribe(client: Subject(element), channel: String)
  Publish(value: element, channel: String)
  SubscriberDown(down: process.Down)
}

pub type Subscriber(element) {
  Subscriber(subject: Subject(element), monitor: process.Monitor)
}

fn handle_message(
  subscribers: dict.Dict(String, List(Subscriber(e))),
  message: Message(e),
) -> actor.Next(dict.Dict(String, List(Subscriber(e))), Message(e)) {
  case message {
    Shutdown -> actor.stop()

    Subscribe(client, channel) -> {
      let monitor = case process.subject_owner(client) {
        Ok(pid) -> process.monitor(pid)
        Error(_) -> {
          panic
          // DIE!
        }
      }

      let new_sub = Subscriber(subject: client, monitor: monitor)

      let updated_subscribers =
        dict.upsert(subscribers, channel, fn(maybe_list) {
          case maybe_list {
            option.None -> [new_sub]
            option.Some(existing) -> [new_sub, ..existing]
          }
        })

      actor.continue(updated_subscribers)
    }

    Unsubscribe(client, channel) -> {
      let updated = case dict.get(subscribers, channel) {
        Ok(sub_list) -> {
          let to_remove = list.filter(sub_list, fn(s) { s.subject == client })

          list.each(to_remove, fn(s) { process.demonitor_process(s.monitor) })

          let filtered = list.filter(sub_list, fn(s) { s.subject != client })

          case filtered {
            [] -> dict.delete(subscribers, channel)
            _ -> dict.insert(subscribers, channel, filtered)
          }
        }
        Error(_) -> subscribers
      }
      actor.continue(updated)
    }

    SubscriberDown(down) -> {
      let cleaned =
        dict.map_values(subscribers, fn(_channel, subs) {
          list.filter(subs, fn(s) {
            case down {
              process.ProcessDown(monitor: m, ..) if m == s.monitor -> False
              _ -> True
            }
          })
        })

      let final = dict.filter(cleaned, fn(_k, v) { !list.is_empty(v) })

      actor.continue(final)
    }

    Publish(value, channel) -> {
      case dict.get(subscribers, channel) {
        Ok(sub_list) -> {
          list.each(sub_list, fn(s) { process.send(s.subject, value) })

          case sub_list {
            [] -> actor.continue(dict.delete(subscribers, channel))
            _ -> actor.continue(subscribers)
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
    |> supervision.significant(True),
  )
  |> supervisor.start()
}
