use pinocchio::{account_info::AccountInfo, entrypoint, pubkey::Pubkey, ProgramResult};

use pinocchio_log::log;
entrypoint!(process_instruction);



pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    log!("Hello, Solana World!");

    if !instruction_data.is_empty() {
        if let Ok(message) = std::str::from_utf8(instruction_data) {
            log!("Received message: {}", message);
        } else {
            log!("Received binary data of length: {}", instruction_data.len());
        }
    }

    log!("Number of accounts: {}", accounts.len());

    log!("Program ID: {}", program_id);

    Ok(())
}

