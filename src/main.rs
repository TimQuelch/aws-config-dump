use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::aot::{Shell, generate};
use output::DuckDbOutput;

mod output;
mod select;

#[derive(Parser)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    Completions { shell: Shell },
    Select,
}

#[tokio::main()]
async fn main() -> anyhow::Result<()> {
    let subscriber = tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .finish();
    tracing::subscriber::set_global_default(subscriber)?;

    let cli = Cli::parse();

    match cli.command {
        Command::Completions { shell } => {
            let mut cmd = Cli::command();
            let name = cmd.get_name().to_owned();
            generate(shell, &mut cmd, name, &mut std::io::stdout());
            Ok(())
        }
        Command::Select => select::select_resources(DuckDbOutput::new()).await,
    }
}
