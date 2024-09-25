use renoir::prelude::*;

/*
TODO:
- implementare il tag nella configurazione
- implementare come connettore redis di default
- impostare la variable env quando spowno con il tag.
- in questo modo sto facendo una da una stram, ha senso? devo creare un qualcosa di più astratto?
- avendo tante stream, una per gruppo, mi basta eseguire la stream con lo stesso gruppo
    host tag: PROVA -> eseguo tutte le stream taggato con PROVA
    host tag: "" -> eseguo tutte le stream senza tag
    let execute = match (host_tag, stream_tag) {
        (Some(a), Some(b)) -> a == b,
        (None, None) -> true,
        _ -> false
    }
- come faccio ad eseguire solamente le stream con un certo tag se ho solamente salvati i blocchi?
    filtro sui blocchi nella funzione dello scheduler che builda le cose Scheduler::schedule_block

// future work
- per ora i nomi devo controllarli a mano, in futuro è giusto avere tutto CamelCase o lowercase o case isensitve
*/

#[test]
fn prova() {
    let config = RuntimeConfig::remote("config.toml")
        .inspect_err(|err| match err {
            renoir::config::ConfigError::Serialization(error) => {
                let content = std::fs::read_to_string("config.toml").unwrap();
                let span = error.span().unwrap();
                println!("substring: {}", &content[span.start..span.end]);
            }
            _ => todo!(),
        })
        .expect("config error");
    config.spawn_remote_workers();

    let context = StreamContext::new(config);

    context
        .deployment_group("gino")
        .stream_iter(0..100)
        .filter(|x| x % 2 == 0)
        .deployment_group("pino")
        .group_by_count(|x| x % 10)
        .collect_vec();

    context.execute_blocking();
}
