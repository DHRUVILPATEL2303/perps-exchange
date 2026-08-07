use pinocchio::{
    account_info::AccountInfo,
    instruction::{AccountMeta, Instruction, Seed, Signer},
    msg,
    program::invoke_signed,
    program_error::ProgramError,
    pubkey::{find_program_address, Pubkey},
    ProgramResult,
};

use pinocchio::entrypoint;
use pinocchio_log::log;

entrypoint!(process_instruction);


pub fn process_instruction(
    program_id: &Pubkey,
    accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    if instruction_data.is_empty() {
        return Err(ProgramError::InvalidInstructionData);
    }

    match instruction_data[0] {
        0 => initialize(program_id, accounts, &instruction_data[1..]),
        1 => sweep(program_id, accounts, &instruction_data[1..]),
        _ => Err(ProgramError::InvalidInstructionData),
    }
}

fn initialize(_program_id: &Pubkey, accounts: &[AccountInfo], data: &[u8]) -> ProgramResult {
    let accounts_iter = &mut accounts.iter();
    let state_account = next_account_info(accounts_iter)?;
    let admin_account = next_account_info(accounts_iter)?;

    if !admin_account.is_signer() {
        return Err(ProgramError::MissingRequiredSignature);
    }

    if data.len() < 32 {
        return Err(ProgramError::InvalidInstructionData);
    }

    let mut treasury_bytes = [0u8; 32];
    treasury_bytes.copy_from_slice(&data[0..32]);
    let treasury_pubkey = Pubkey::from(treasury_bytes);

    let mut state_data = state_account.try_borrow_mut_data()?;
    if state_data.len() < 64 {
        return Err(ProgramError::AccountDataTooSmall);
    }

    state_data[0..32].copy_from_slice(admin_account.key().as_ref());
    state_data[32..64].copy_from_slice(treasury_pubkey.as_ref());

    msg!("Exchange custody initialized successfully.");
    Ok(())
}

fn sweep(program_id: &Pubkey, accounts: &[AccountInfo], data: &[u8]) -> ProgramResult {
    let accounts_iter = &mut accounts.iter();

    let state_account = next_account_info(accounts_iter)?;
    let admin_account = next_account_info(accounts_iter)?;
    let user_pda_token = next_account_info(accounts_iter)?;
    let treasury_token = next_account_info(accounts_iter)?;
    let token_program = next_account_info(accounts_iter)?;

    if !admin_account.is_signer() {
        return Err(ProgramError::MissingRequiredSignature);
    }

    let state_data = state_account.try_borrow_data()?;
    if state_data.len() < 64 {
        return Err(ProgramError::InvalidAccountData);
    }

    let mut admin_bytes = [0u8; 32];
    admin_bytes.copy_from_slice(&state_data[0..32]);
    let stored_admin = Pubkey::from(admin_bytes);

    let mut treasury_bytes = [0u8; 32];
    treasury_bytes.copy_from_slice(&state_data[32..64]);
    let stored_treasury = Pubkey::from(treasury_bytes);

    if admin_account.key() != &stored_admin {
        return Err(ProgramError::InvalidAccountData);
    }

    if data.len() < 16 {
        return Err(ProgramError::InvalidInstructionData);
    }
    let mut user_uuid = [0u8; 16];
    user_uuid.copy_from_slice(&data[0..16]);

    let (expected_pda, bump) =
        find_program_address(&[b"user_deposit", &user_uuid as &[u8]], program_id);

    let pda_token_data = user_pda_token.try_borrow_data()?;
    if pda_token_data.len() < 72 {
        return Err(ProgramError::InvalidAccountData);
    }

    let mut mint_bytes = [0u8; 32];
    mint_bytes.copy_from_slice(&pda_token_data[0..32]);
    let token_mint = Pubkey::from(mint_bytes);

    let mut owner_bytes = [0u8; 32];
    owner_bytes.copy_from_slice(&pda_token_data[32..64]);
    let token_owner = Pubkey::from(owner_bytes);

    if token_owner != expected_pda {
        return Err(ProgramError::InvalidAccountData);
    }

    let mut amount_bytes = [0u8; 8];
    amount_bytes.copy_from_slice(&pda_token_data[64..72]);
    let amount = u64::from_le_bytes(amount_bytes);

    if amount == 0 {
        msg!("No funds to sweep.");
        return Ok(());
    }

    let treasury_token_data = treasury_token.try_borrow_data()?;
    if treasury_token_data.len() < 64 {
        return Err(ProgramError::InvalidAccountData);
    }

    let mut t_mint_bytes = [0u8; 32];
    t_mint_bytes.copy_from_slice(&treasury_token_data[0..32]);
    let treasury_mint = Pubkey::from(t_mint_bytes);

    let mut t_owner_bytes = [0u8; 32];
    t_owner_bytes.copy_from_slice(&treasury_token_data[32..64]);
    let treasury_owner = Pubkey::from(t_owner_bytes);

    if treasury_owner != stored_treasury {
        return Err(ProgramError::InvalidAccountData);
    }

    if token_mint != treasury_mint {
        return Err(ProgramError::InvalidAccountData);
    }

    let mut transfer_data = [0u8; 9];
    transfer_data[0] = 3;
    transfer_data[1..9].copy_from_slice(&amount.to_le_bytes());

    let bump_seed = [bump];
    
    let signer_seeds = [
        Seed::from(b"user_deposit"),
        Seed::from(&user_uuid),
        Seed::from(&bump_seed),
    ];

    let signer = [Signer::from(&signer_seeds[..])];


    

    let metas = [
        AccountMeta::new(&*user_pda_token.key(), false, true),
        AccountMeta::new(&*treasury_token.key(), false, true),
        AccountMeta::readonly(&expected_pda),
    ];

    let sweep_ix = Instruction {
        program_id: token_program.key(),
        accounts: &metas,
        data: &transfer_data,
    };

    invoke_signed(
        &sweep_ix,
        &[&*user_pda_token, &*treasury_token],
        &signer,
    )?;

    log!("Successfully swept {} tokens from PDA to treasury.", amount);

    Ok(())
}

fn next_account_info<'a, I>(iter: &mut I) -> Result<&'a AccountInfo, ProgramError>
where
    I: Iterator<Item = &'a AccountInfo>,
{
    iter.next().ok_or(ProgramError::NotEnoughAccountKeys)
}
