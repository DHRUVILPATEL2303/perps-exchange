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

const SYSTEM_PROGRAM_ID: [u8; 32] = [0u8; 32];

const SPL_TOKEN_PROGRAM_ID: [u8; 32] = [
    6, 221, 246, 225, 215, 101, 161, 147, 217, 203, 225, 70, 206, 235, 121, 172, 28, 180, 133, 237,
    95, 91, 55, 145, 58, 140, 245, 133, 126, 255, 0, 169,
];

const STATE_SEED: &[u8] = b"custody_state_v8";

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

fn initialize(program_id: &Pubkey, accounts: &[AccountInfo], data: &[u8]) -> ProgramResult {
    let accounts_iter = &mut accounts.iter();
    let state_account = next_account_info(accounts_iter)?;
    let admin_account = next_account_info(accounts_iter)?;
    let system_program = next_account_info(accounts_iter)?;

    if !admin_account.is_signer() {
        return Err(ProgramError::MissingRequiredSignature);
    }

    if system_program.key() != &Pubkey::from(SYSTEM_PROGRAM_ID) {
        return Err(ProgramError::IncorrectProgramId);
    }

    // data layout: usdc_treasury(32) + usdt_treasury(32) + rent(8) + bump(1) = 73 bytes
    if data.len() < 73 {
        return Err(ProgramError::InvalidInstructionData);
    }

    let (expected_state, bump_check) = find_program_address(&[STATE_SEED], program_id);
    if state_account.key() != &expected_state {
        return Err(ProgramError::InvalidAccountData);
    }

    let mut usdc_treasury_bytes = [0u8; 32];
    usdc_treasury_bytes.copy_from_slice(&data[0..32]);
    let usdc_treasury = Pubkey::from(usdc_treasury_bytes);

    let mut usdt_treasury_bytes = [0u8; 32];
    usdt_treasury_bytes.copy_from_slice(&data[32..64]);
    let usdt_treasury = Pubkey::from(usdt_treasury_bytes);

    let rent_lamports = u64::from_le_bytes(
        data[64..72]
            .try_into()
            .map_err(|_| ProgramError::InvalidInstructionData)?,
    );

    let bump = data[72];
    if bump != bump_check {
        return Err(ProgramError::InvalidInstructionData);
    }

    let mut create_data = [0u8; 52];
    create_data[0..4].copy_from_slice(&0u32.to_le_bytes());
    create_data[4..12].copy_from_slice(&rent_lamports.to_le_bytes());
    create_data[12..20].copy_from_slice(&96u64.to_le_bytes()); // 96 bytes: admin(32) + usdc(32) + usdt(32)
    create_data[20..52].copy_from_slice(program_id.as_ref());

    let metas = [
        AccountMeta::new(admin_account.key(), true, true),
        AccountMeta::new(state_account.key(), true, true),
    ];

    let create_ix = Instruction {
        program_id: system_program.key(),
        accounts: &metas,
        data: &create_data,
    };

    let bump_seed = [bump];
    let signer_seeds = [Seed::from(STATE_SEED), Seed::from(&bump_seed)];
    let signer = [Signer::from(&signer_seeds[..])];

    invoke_signed(&create_ix, &[admin_account, state_account], &signer)?;

    let mut state_data = state_account.try_borrow_mut_data()?;
    state_data[0..32].copy_from_slice(admin_account.key().as_ref());
    state_data[32..64].copy_from_slice(usdc_treasury.as_ref());
    state_data[64..96].copy_from_slice(usdt_treasury.as_ref());

    log!("Exchange custody initialized successfully.");
    Ok(())
}

fn sweep(program_id: &Pubkey, accounts: &[AccountInfo], data: &[u8]) -> ProgramResult {
    let accounts_iter = &mut accounts.iter();

    let state_account = next_account_info(accounts_iter)?;
    let admin_account = next_account_info(accounts_iter)?;
    let user_pda_token = next_account_info(accounts_iter)?;
    let treasury_token = next_account_info(accounts_iter)?;
    let token_program = next_account_info(accounts_iter)?;
    let user_pda_authority = next_account_info(accounts_iter)?;

    if !admin_account.is_signer() {
        return Err(ProgramError::MissingRequiredSignature);
    }

    if token_program.key() != &Pubkey::from(SPL_TOKEN_PROGRAM_ID) {
        return Err(ProgramError::IncorrectProgramId);
    }

    let (expected_state, _) = find_program_address(&[STATE_SEED], program_id);
    if state_account.key() != &expected_state {
        log!("Error: State account mismatch");

        return Err(ProgramError::InvalidAccountData);
    }

    let state_data = state_account.try_borrow_data()?;
    if state_data.len() < 96 {
        log!("Error: State data length too short");
        return Err(ProgramError::InvalidAccountData);
    }

    let mut admin_bytes = [0u8; 32];
    admin_bytes.copy_from_slice(&state_data[0..32]);
    let stored_admin = Pubkey::from(admin_bytes);

    let mut usdc_treasury_bytes = [0u8; 32];
    usdc_treasury_bytes.copy_from_slice(&state_data[32..64]);
    let stored_usdc_treasury = Pubkey::from(usdc_treasury_bytes);

    let mut usdt_treasury_bytes = [0u8; 32];
    usdt_treasury_bytes.copy_from_slice(&state_data[64..96]);
    let stored_usdt_treasury = Pubkey::from(usdt_treasury_bytes);

    if admin_account.key() != &stored_admin {
        log!("Error: Admin account mismatch");
        return Err(ProgramError::InvalidAccountData);
    }

    if data.len() < 16 {
        return Err(ProgramError::InvalidInstructionData);
    }
    let mut user_uuid = [0u8; 16];
    user_uuid.copy_from_slice(&data[0..16]);

    let (expected_pda, bump) =
        find_program_address(&[b"user_deposit", &user_uuid as &[u8]], program_id);

    if user_pda_authority.key() != &expected_pda {
        log!("Error: User PDA authority mismatch");
        return Err(ProgramError::InvalidAccountData);
    }

    let (token_mint, amount) = {
        let pda_token_data = user_pda_token.try_borrow_data()?;
        if pda_token_data.len() < 72 {
            log!("Error: User PDA token account uninitialized or too small");
            return Err(ProgramError::InvalidAccountData);
        }

        let mut mint_bytes = [0u8; 32];
        mint_bytes.copy_from_slice(&pda_token_data[0..32]);
        let token_mint = Pubkey::from(mint_bytes);

        let mut owner_bytes = [0u8; 32];
        owner_bytes.copy_from_slice(&pda_token_data[32..64]);
        let token_owner = Pubkey::from(owner_bytes);

        if token_owner != expected_pda {
            log!("Error: User token account owner mismatch");
            return Err(ProgramError::InvalidAccountData);
        }

        let mut amount_bytes = [0u8; 8];
        amount_bytes.copy_from_slice(&pda_token_data[64..72]);
        let amount = u64::from_le_bytes(amount_bytes);

        (token_mint, amount)
    };

    if amount == 0 {
        log!("No funds to sweep.");
        return Ok(());
    }

    {
        let treasury_token_data = treasury_token.try_borrow_data()?;
        if treasury_token_data.len() < 64 {
            log!("Error: Treasury token account uninitialized or too small");
            return Err(ProgramError::InvalidAccountData);
        }

        let mut t_mint_bytes = [0u8; 32];
        t_mint_bytes.copy_from_slice(&treasury_token_data[0..32]);
        let treasury_mint = Pubkey::from(t_mint_bytes);

        if treasury_token.key() != &stored_usdc_treasury
            && treasury_token.key() != &stored_usdt_treasury
        {
            log!("Error: Treasury token mismatch. Does not match USDC or USDT treasury.");
            return Err(ProgramError::InvalidAccountData);
        }

        if token_mint != treasury_mint {
            log!("Error: Token mint mismatch");
            return Err(ProgramError::InvalidAccountData);
        }
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
        AccountMeta::new(&*user_pda_token.key(), true, false),
        AccountMeta::new(&*treasury_token.key(), true, false),
        AccountMeta::new(user_pda_authority.key(), false, true),
    ];

    let sweep_ix = Instruction {
        program_id: token_program.key(),
        accounts: &metas,
        data: &transfer_data,
    };

    invoke_signed(
        &sweep_ix,
        &[&*user_pda_token, &*treasury_token, user_pda_authority],
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
