#![type_length_limit = "2861949"]
use clap::{crate_version, App, Arg};

use tifs::mount_tifs;
use tifs::MountOption;
use tracing_libatrace as tracing_atrace;
use tracing_subscriber::{layer::SubscriberExt, registry::Registry};

#[async_std::main]
async fn main() {
    let matches = App::new("TiFS")
        .version(crate_version!())
        .author("Hexi Lee")
        .arg(
            Arg::with_name("pd")
                .long("pd-endpoints")
                .short("p")
                .multiple(true)
                .value_name("ENDPOINTS")
                .default_value("127.0.0.1:2379")
                .help("set all pd endpoints of the tikv cluster")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("mount-point")
                .long("mount-point")
                .short("m")
                .value_name("MOUNT_POINT")
                .required(true)
                .help("Act as a client, and mount FUSE at given path")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("options")
                .value_name("OPTION")
                .long("option")
                .short("o")
                .multiple(true)
                .help("filesystem mount options"),
        )
        .get_matches();

    setup_global_subscriber();

    let endpoints: Vec<&str> = matches
        .values_of("pd")
        .unwrap_or_default()
        .to_owned()
        .collect();
    let mountpoint: String = matches.value_of("mount-point").unwrap().to_string();
    let options = MountOption::to_vec(matches.values_of("options").unwrap_or_default());
    mount_tifs(mountpoint, endpoints, options).await.unwrap();
}

fn setup_global_subscriber() {
    let layer = tracing_atrace::layer()
        .unwrap()
        .with_data_field(Option::Some("data".to_string()));
    let subscriber = Registry::default().with(layer);
    tracing::subscriber::set_global_default(subscriber).unwrap();
}
