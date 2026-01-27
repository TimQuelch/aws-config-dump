use aws_sdk_config::types::ResourceType;
use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::aot::{Shell, generate};
use output::JsonFileOutput;

mod list;
mod output;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    List { types: Vec<ResourceType> },
    Configs { types: Vec<ResourceType> },
    Completions { shell: Shell },
}

#[tokio::main()]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::FmtSubscriber::new();
    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    match cli.command {
        Command::Completions { shell } => {
            let mut cmd = Cli::command();
            let name = cmd.get_name().to_owned();
            generate(shell, &mut cmd, name, &mut std::io::stdout());
            Ok(())
        }
        Command::List { types } => list::list(&types).await,
        // Command::Configs { types } => list::resource_configs(&types, StdoutOutput::new()).await,
        Command::Configs { types } => list::resource_configs(&types, JsonFileOutput::new()).await,
    }
}
