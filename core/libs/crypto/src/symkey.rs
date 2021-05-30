extern crate rand;

use aead::{generic_array::GenericArray, Aead, NewAead};
use aes_gcm::Aes256Gcm;

use lockbook_models::crypto::*;

use self::rand::rngs::OsRng;
use self::rand::RngCore;

use serde::de::DeserializeOwned;
use serde::Serialize;

pub fn generate_key() -> AESKey {
    let mut random_bytes = [0u8; 32];
    OsRng.fill_bytes(&mut random_bytes);
    random_bytes
}

#[derive(Debug)]
pub enum AESEncryptError {
    Serialization(bincode::Error),
    Encryption(aead::Error),
}

pub fn encrypt<T: Serialize + DeserializeOwned>(
    key: &AESKey,
    to_encrypt: &T,
) -> Result<AESEncrypted<T>, AESEncryptError> {
    let serialized = bincode::serialize(to_encrypt).map_err(AESEncryptError::Serialization)?;
    let nonce = &generate_nonce();
    let encrypted = convert_key(key)
        .encrypt(
            &GenericArray::from_slice(nonce),
            aead::Payload {
                msg: &serialized,
                aad: &[],
            },
        )
        .map_err(AESEncryptError::Encryption)?;
    Ok(AESEncrypted::new(encrypted, nonce.to_vec()))
}

#[derive(Debug)]
pub enum AESDecryptError {
    Decryption(aead::Error),
    Deserialization(bincode::Error),
}

pub fn decrypt<T: DeserializeOwned>(
    key: &AESKey,
    to_decrypt: &AESEncrypted<T>,
) -> Result<T, AESDecryptError> {
    let nonce = GenericArray::from_slice(&to_decrypt.nonce);
    let decrypted = convert_key(key)
        .decrypt(
            &nonce,
            aead::Payload {
                msg: &to_decrypt.value,
                aad: &[],
            },
        )
        .map_err(AESDecryptError::Decryption)?;
    let deserialized =
        bincode::deserialize(&decrypted).map_err(AESDecryptError::Deserialization)?;
    Ok(deserialized)
}

fn convert_key(to_convert: &AESKey) -> Aes256Gcm {
    Aes256Gcm::new(GenericArray::clone_from_slice(to_convert))
}

fn generate_nonce() -> [u8; 12] {
    let mut result = [0u8; 12];
    OsRng.fill_bytes(&mut result);
    result
}

#[cfg(test)]
mod unit_test_symmetric {
    use uuid::Uuid;

    use crate::symkey::{decrypt, encrypt, generate_key};

    #[test]
    fn test_key_generation() {
        let key = generate_key();
        let test_value = Uuid::new_v4().to_string();
        let encrypted = encrypt(&key, &test_value).unwrap();
        let decrypted = decrypt(&key, &encrypted).unwrap();
        assert_eq!(test_value, decrypted)
    }
}