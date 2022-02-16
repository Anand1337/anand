use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::{env, metadata, near_bindgen, AccountId};
use std::collections::HashMap;

metadata! {
#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
pub struct GetSet {
        records: HashMap<AccountId,i64>,
}

#[near_bindgen]
impl GetSet{
    pub fn set(&mut self, value: i64) {
            let account_id = env::signer_account_id();
          self.records.insert(account_id, value);
    }

    pub fn get(&self, account_id: AccountId) -> Option::<i64> {
         self.records.get(&account_id).cloned()
    }

    pub fn minimum(&self) -> Option::<(String,i64)> {
      self.records.iter().min_by_key(|(_,v)|v.clone()).map(|(a,b)|(a.clone(),b.clone()))
    }
}
    }

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod tests {
    use super::*;
    use near_sdk::json_types::ValidAccountId;
    use near_sdk::test_utils::VMContextBuilder;
    use near_sdk::MockedBlockchain;
    use near_sdk::{testing_env, VMContext};

    fn get_context(is_view: bool) -> VMContext {
        VMContextBuilder::new()
            .signer_account_id(ValidAccountId::try_from("bob_near").unwrap())
            .is_view(is_view)
            .build()
    }

    #[test]
    fn set_get() {
        let context = get_context(false);
        testing_env!(context);
        let mut contract = GetSet::default();
        contract.set(5);
        let context = get_context(true);
        testing_env!(context);
        assert_eq!(Some(5), contract.get("bob_near".to_string()));
        assert_eq!(Some(("bob_near".to_string(), 5)), contract.minimum());
    }

    #[test]
    fn get_before_set() {
        let context = get_context(true);
        testing_env!(context);
        let contract = GetSet::default();
        assert_eq!(None, contract.get("bob_near".to_string()));
        assert_eq!(None, contract.minimum());
    }
}
