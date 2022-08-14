use crate::config::Config;
use crate::{ServerError, FREE_TIER_USAGE_SIZE, PREMIUM_TIER_USAGE_SIZE};
use google_androidpublisher3::api::SubscriptionPurchase;
use lockbook_shared::api::{GooglePlayAccountState, UnixTimeMillis, UpgradeAccountGooglePlayError};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use uuid::Uuid;

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct SubscriptionProfile {
    pub billing_platform: Option<BillingPlatform>,
    pub last_in_payment_flow: u64,
}

impl SubscriptionProfile {
    pub fn data_cap(&self) -> u64 {
        match &self.billing_platform {
            Some(platform) => match platform {
                BillingPlatform::Stripe(_) => PREMIUM_TIER_USAGE_SIZE,
                BillingPlatform::GooglePlay(info) => {
                    if info.account_state == GooglePlayAccountState::OnHold {
                        FREE_TIER_USAGE_SIZE
                    } else {
                        PREMIUM_TIER_USAGE_SIZE
                    }
                }
            },
            None => FREE_TIER_USAGE_SIZE,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum BillingPlatform {
    Stripe(StripeUserInfo),
    GooglePlay(GooglePlayUserInfo),
}

impl BillingPlatform {
    pub fn new_play_sub(
        config: &Config, purchase_token: &str, expiry_information: SubscriptionPurchase,
    ) -> Result<Self, ServerError<UpgradeAccountGooglePlayError>> {
        let expiration_time = expiry_information
            .expiry_time_millis
            .ok_or_else::<ServerError<UpgradeAccountGooglePlayError>, _>(|| {
                internal!("Cannot get expiration time of a recovered subscription.")
            })?
            .parse()
            .map_err::<ServerError<UpgradeAccountGooglePlayError>, _>(|e| {
                internal!("Cannot parse millis into int: {:?}", e)
            })?;

        Ok(Self::GooglePlay(GooglePlayUserInfo {
            purchase_token: purchase_token.to_string(),
            subscription_product_id: config
                .billing
                .google
                .premium_subscription_product_id
                .clone(),
            subscription_offer_id: config.billing.google.premium_subscription_offer_id.clone(),
            expiration_time,
            account_state: GooglePlayAccountState::Ok,
        }))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GooglePlayUserInfo {
    pub purchase_token: String,
    pub subscription_product_id: String,
    pub subscription_offer_id: String,
    pub expiration_time: UnixTimeMillis,
    pub account_state: GooglePlayAccountState,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct StripeUserInfo {
    pub customer_id: String,
    pub customer_name: Uuid,
    pub price_id: String,
    pub payment_method_id: String,
    pub last_4: String,
    pub subscription_id: String,
    pub expiration_time: UnixTimeMillis,
}