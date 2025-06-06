use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2;
use secstr::SecUtf8;
use sha2::{Digest, Sha256};

pub(crate) fn scram_sha256(
    salt: &[u8],
    server_key: &[u8],
    client_challenge: &[u8],
    password: &SecUtf8,
) -> Result<(Vec<u8>, Vec<u8>), crypto_common::InvalidLength> {
    let salted_password = hmac(password.unsecure().as_ref(), salt)?;

    let (s, sk, cc) = (salt.len(), server_key.len(), client_challenge.len());
    let mut content: Vec<u8> = std::iter::repeat_n(0, s + sk + cc).collect();
    content[0..s].copy_from_slice(salt);
    content[s..(s + sk)].copy_from_slice(server_key);
    content[(s + sk)..].copy_from_slice(client_challenge);

    let client_key: Vec<u8> = sha256(&salted_password);
    let sig: Vec<u8> = hmac(&sha256(&client_key), &content)?;

    let client_proof = xor(&sig, &client_key);

    // calculate server proof
    let ck = client_key.len();
    let mut content2: Vec<u8> = std::iter::repeat_n(0, s + sk + ck).collect();

    content2[0..ck].copy_from_slice(&client_key);
    content2[ck..(ck + s)].copy_from_slice(salt);
    content2[(ck + s)..].copy_from_slice(server_key);

    let server_verifier = hmac(&salted_password, salt)?;
    let server_proof = hmac(&server_verifier, &content2)?;

    Ok((client_proof, server_proof))
}

pub fn scram_pdkdf2_sha256(
    salt: &[u8],
    server_nonce: &[u8],
    client_nonce: &[u8],
    password: &SecUtf8,
    iterations: u32,
) -> Result<(Vec<u8>, Vec<u8>), crypto_common::InvalidLength> {
    let salted_password = use_pbkdf2(password.unsecure().as_ref(), salt, iterations);

    let server_verifier = hmac(&salted_password, salt)?;

    let client_key = sha256(&salted_password);
    let client_verifier = sha256(&client_key);

    let (s, sn, cn) = (salt.len(), server_nonce.len(), client_nonce.len());
    let mut s_sn_cn: Vec<u8> = std::iter::repeat_n(0, s + sn + cn).collect();
    s_sn_cn[0..s].copy_from_slice(salt);
    s_sn_cn[s..(s + sn)].copy_from_slice(server_nonce);
    s_sn_cn[(s + sn)..].copy_from_slice(client_nonce);
    let shared_key: Vec<u8> = hmac(&client_verifier, &s_sn_cn)?;
    let client_proof = xor(&shared_key, &client_key);

    let mut cn_s_sn: Vec<u8> = std::iter::repeat_n(0, cn + s + sn).collect();
    cn_s_sn[0..cn].copy_from_slice(client_nonce);
    cn_s_sn[cn..(cn + s)].copy_from_slice(salt);
    cn_s_sn[(cn + s)..].copy_from_slice(server_nonce);
    let server_proof = hmac(&server_verifier, &cn_s_sn)?;

    Ok((client_proof, server_proof))
}

pub fn use_pbkdf2(key: &[u8], salt: &[u8], it: u32) -> Vec<u8> {
    let mut output = [0_u8; 32];

    pbkdf2::<Hmac<Sha256>>(key, salt, it, &mut output)
    .unwrap(/* OK - invalid length should not be possible */);
    output.to_vec()
}

fn hmac(key: &[u8], data: &[u8]) -> Result<Vec<u8>, crypto_common::InvalidLength> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

pub fn sha256(input: &[u8]) -> Vec<u8> {
    let mut sha = Sha256::new();
    sha.update(input);
    sha.finalize().to_vec()
}

pub fn xor(a: &[u8], b: &[u8]) -> Vec<u8> {
    assert_eq!(a.len(), b.len(), "xor needs two equally long parameters");

    let mut bytes: Vec<u8> = std::iter::repeat_n(0_u8, a.len()).collect();
    for i in 0..a.len() {
        bytes[i] = a[i] ^ b[i];
    }
    bytes
}
