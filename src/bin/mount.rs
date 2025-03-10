use clap::{crate_version, App, Arg};

use tifs::mount_tifs_daemonize;
use tifs::MountOption;
use tracing::{debug, info, trace};
use tracing_libatrace as tracing_atrace;
use tracing_subscriber::{layer::SubscriberExt, registry::Registry};

#[async_std::main]
async fn main() {
    let matches = App::new("mount.tifs")
        .version(crate_version!())
        .author("Hexi Lee")
        .arg(
            Arg::with_name("device")
                .value_name("ENDPOINTS")
                .required(true)
                .help("all pd endpoints of the tikv cluster, separated by commas (e.g. tifs:127.0.0.1:2379)")
                .index(1)
        )
        .arg(
            Arg::with_name("mount-point")
                .value_name("MOUNT_POINT")
                .required(true)
                .help("Act as a client, and mount FUSE at given path")
                .index(2)
        )
        .arg(
            Arg::with_name("options")
                .value_name("OPTION")
                .long("option")
                .short("o")
                .multiple(true)
                .help("filesystem mount options")
        )
        .arg(
            Arg::with_name("foreground")
                .long("foreground")
                .short("f")
                .help("foreground operation")
        )
        .arg(
            Arg::with_name("serve")
                .long("serve")
                .help("run in server mode (implies --foreground)")
                .hidden(true)
        )
        .arg(
            Arg::with_name("logfile")
                .long("log-file")
                .value_name("LOGFILE")
                .help("log file in server mode (ignored if --foreground is present)")
        )
        .get_matches();

    setup_global_subscriber();

    let serve = matches.is_present("serve");
    let foreground = serve || matches.is_present("foreground");
    let logfile = matches.value_of("logfile").map(|v| {
        std::fs::canonicalize(v)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned()
    });

    trace!("serve={} foreground={}", serve, foreground);

    let device = matches.value_of("device").unwrap_or_default();

    let endpoints: Vec<&str> = device
        .strip_prefix("tifs:")
        .unwrap_or(device)
        .split(",")
        .collect();

    let mountpoint: String =
        std::fs::canonicalize(matches.value_of("mount-point").unwrap().to_string())
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();

    let options = MountOption::to_vec(matches.values_of("options").unwrap_or_default());

    let runtime_config_string = format!(
        "mountpoint={:?} endpoints={:?} opt={:?}",
        mountpoint, endpoints, options
    );

    if !foreground {
        use std::io::{Read, Write};
        use std::process::{Command, Stdio};

        let exe = std::env::current_exe()
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
        debug!("Launching server, current_exe={}", exe);
        info!("{}", runtime_config_string);

        let mut args = vec![
            "--serve".to_owned(),
            format!("tifs:{}", endpoints.join(",")),
            mountpoint,
        ];
        if options.len() > 0 {
            args.push("-o".to_owned());
            args.push(
                options
                    .iter()
                    .map(|v| v.into())
                    .collect::<Vec<String>>()
                    .join(","),
            );
        }
        if let Some(f) = logfile {
            args.push("--log-file".to_owned());
            args.push(f);
        }
        let child = Command::new(exe)
            .args(args)
            .current_dir("/")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();

        if let Some(mut stdout) = child.stdout {
            let mut my_stdout = std::io::stdout();
            let mut buffer: [u8; 256] = [0; 256];
            while let Ok(size) = stdout.read(&mut buffer) {
                if size == 0 {
                    break; // EOF
                }
                my_stdout.write_all(&buffer[0..size]).unwrap();
            }
        }
        if let Some(mut stderr) = child.stderr {
            let mut my_stderr = std::io::stderr();
            let mut buffer: [u8; 256] = [0; 256];
            while let Ok(size) = stderr.read(&mut buffer) {
                if size == 0 {
                    break; // EOF
                }
                my_stderr.write_all(&buffer[0..size]).unwrap();
            }
        }
        return;
    }

    mount_tifs_daemonize(mountpoint, endpoints, options, move || {
        if serve {
            use anyhow::bail;
            use libc;
            use std::ffi::CString;
            use std::io::Error;
            use std::io::Write;

            debug!("Using log file: {:?}", logfile);

            std::io::stdout().flush()?;
            std::io::stderr().flush()?;

            let mut logfd = None;
            if let Some(f) = logfile {
                let log_file_name = CString::new(f)?;
                unsafe {
                    let fd = libc::open(log_file_name.as_ptr(), libc::O_WRONLY | libc::O_APPEND, 0);
                    if fd == -1 {
                        bail!(Error::last_os_error());
                    }
                    logfd = Some(fd);

                    libc::dup2(fd, 1);
                    libc::dup2(fd, 2);
                    if fd > 2 {
                        libc::close(fd);
                    }
                }
                debug!("output redirected");
            }

            let null_file_name = CString::new("/dev/null")?;

            unsafe {
                let nullfd = libc::open(null_file_name.as_ptr(), libc::O_RDWR, 0);
                if nullfd != -1 {
                    libc::dup2(nullfd, 0);
                    if logfd.is_none() {
                        libc::dup2(nullfd, 1);
                        libc::dup2(nullfd, 2);
                    }
                    if nullfd > 2 {
                        libc::close(nullfd);
                    }
                }
            }
        }
        debug!("{}", runtime_config_string);

        Ok(())
    })
    .await
    .unwrap();
}

fn setup_global_subscriber() {
    let layer = tracing_atrace::layer()
        .unwrap()
        .with_data_field(Option::Some("data".to_string()));
    let subscriber = Registry::default().with(layer);
    tracing::subscriber::set_global_default(subscriber).unwrap();
}
