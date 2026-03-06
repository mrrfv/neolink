//! Main test app for mail

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use neolink_core::{
    bc::xml::{xml_ver, Email},
    bc_protocol::BcCamera,
};
use std::{
    fs,
    net::{IpAddr, SocketAddr},
};
use validator::Validate;

mod config;
mod opt;
mod utils;

use config::Config;
use opt::Opt;
use utils::find_and_connect;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info"))
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "{} {} [{}] {}",
                buf.timestamp_millis(),
                record.level(),
                record.module_path().unwrap_or("unknown"),
                record.args()
            )
        })
        .init();

    let opt = Opt::parse();

    let conf_path = opt.config.context("Must supply --config file")?;
    let config: Config = toml::from_str(
        &fs::read_to_string(&conf_path)
            .with_context(|| format!("Failed to read {:?}", conf_path))?,
    )
    .with_context(|| format!("Failed to parse the {:?} config file", conf_path))?;

    config
        .validate()
        .with_context(|| format!("Failed to validate the {:?} config file", conf_path))?;

    let name = opt.camera.clone();
    let camera = find_and_connect(&config, &opt.camera).await?;

    const MAIL_PORT: u16 = 22022;
    // let addr = SocketAddr::from((get_local_ip()?, MAIL_PORT));
    let cam_addr = SocketAddr::from((IpAddr::from([192, 168, 1, 201]), MAIL_PORT));
    let post_addr = SocketAddr::from((IpAddr::from([127, 0, 0, 1]), MAIL_PORT));

    tokio::select! {
        v = cam_tasks(&name, camera, cam_addr) => v,
        v = mail_server(&name, post_addr) => v,
    }?;

    Ok(())
}

async fn cam_tasks(name: &str, camera: BcCamera, addr: SocketAddr) -> Result<()> {
    let support = camera.get_support().await?;
    if support.email.is_some_and(|v| v > 0) {
        let ip = addr.ip();
        let port = addr.port();
        log::info!("Telling camera to send the mail to {}:{}", ip, port);
        let mail_settings = Email {
            version: xml_ver(),
            smtp_server: format!("{}", ip),
            user_name: format!("{name}@neolink.neolink"),
            password: "TestPass".to_owned(),
            address1: "neolink@neolink.neolink".to_owned(),
            address2: "".to_owned(),
            address3: "".to_owned(),
            smtp_port: addr.port(),
            send_nickname: name.to_string(),
            attachment: 1,
            attachment_type: Some("picture".to_owned()),
            text_type: "withText".to_owned(),
            ssl: 0,
            interval: 30,
            sender_max_len: None,
        };
        camera.set_email(mail_settings.clone()).await?;
        camera.email_on_always().await?;

        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
        log::info!("Sending test email");
        camera.test_email(mail_settings).await?;

        // Wait to give me time to wave at the camera please
        tokio::time::sleep(tokio::time::Duration::from_secs(300)).await;

        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "Camera does not support email notfications"
        ))
    }
}

use mailin_embedded::{
    response::{self, Response},
    Handler, Server, SslConfig,
};
use regex::Regex;

#[derive(Clone)]
struct MailHandler {
    name: String,
}
impl Handler for MailHandler {
    fn helo(&mut self, _ip: IpAddr, _domain: &str) -> Response {
        log::debug!("HELO");
        response::OK
    }

    fn rcpt(&mut self, to: &str) -> Response {
        log::debug!("rcpr:: {to}");
        let re = Regex::new(r"^(.*)@neolink.neolink$").unwrap();
        if re.is_match(to) {
            response::OK
        } else {
            response::NO_MAILBOX
        }
    }

    fn mail(&mut self, ip: IpAddr, domain: &str, from: &str) -> Response {
        log::debug!("mail:: ip: {ip:?}, domain: {domain}, from: {from}");
        response::OK
    }

    fn data_start(&mut self, domain: &str, from: &str, is8bit: bool, to: &[String]) -> Response {
        log::debug!("data_start:: domain: {domain}, from: {from}, is8bit: {is8bit}, to: {to:?}");
        response::OK
    }

    fn data(&mut self, buf: &[u8]) -> std::io::Result<()> {
        let text = String::from_utf8_lossy(buf);
        log::debug!("data:: text: {text}");
        Ok(())
    }

    fn data_end(&mut self) -> Response {
        log::debug!("data_end::");
        response::OK
    }

    fn auth_login(&mut self, username: &str, password: &str) -> Response {
        log::debug!("auth_login;: username: {username}, password: {password}");
        let correct_username = format!("{}@neolink.neolink", self.name);
        let correct_password = "TestPass";
        if username == correct_username && password == correct_password {
            response::AUTH_OK
        } else {
            response::INVALID_CREDENTIALS
        }
    }

    fn auth_plain(
        &mut self,
        authorization_id: &str,
        authentication_id: &str,
        password: &str,
    ) -> Response {
        log::debug!("auth_plain:: authorization_id: {authorization_id}, authentication_id: {authentication_id}, password: {password}");
        response::INVALID_CREDENTIALS
    }
}

async fn mail_server(name: &str, addr: SocketAddr) -> Result<()> {
    let handler = MailHandler {
        name: name.to_string(),
    };
    let mut server = Server::new(handler);

    server
        .with_name("neolink.neolink")
        .with_ssl(SslConfig::None)
        .map_err(|e| anyhow!("{e:?}"))?
        .with_addr(addr)
        .map_err(|e| anyhow!("{e:?}"))?;

    tokio::task::spawn_blocking(move || server.serve().map_err(|e| anyhow!("{e:?}"))).await??;
    Ok(())
}
