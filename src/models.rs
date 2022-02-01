use crate::proto;
use rust_decimal::Decimal;
use std::hash::Hash;
use std::iter::Iterator;

/// Module defines transactor data model.

/// Type-safe client id.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct ClientId(u16);

impl ClientId {
    pub fn new(inner: u16) -> ClientId {
        ClientId(inner)
    }
}

/// Type-safe transaction id.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct TransactionId(u32);

impl TransactionId {
    pub fn new(inner: u32) -> TransactionId {
        TransactionId(inner)
    }
}

/// Transaction meta information.
#[derive(Debug, Clone)]
pub struct Meta {
    pub client_id: ClientId,
    pub transaction_id: TransactionId,
}

/// Transaction model.
#[derive(Debug, Clone)]
pub enum Transaction {
    Deposit { meta: Meta, amount: Decimal },
    Withdrawal { meta: Meta, amount: Decimal },
    Dispute { meta: Meta },
    Resolve { meta: Meta },
    Chargeback { meta: Meta },
}

impl Transaction {
    /// Reads transactions from a given `csv::Reader`.
    pub fn read_many<'a, T: std::io::Read>(
        reader: &'a mut csv::Reader<T>,
    ) -> Box<dyn Iterator<Item = Result<Transaction, proto::ParseError>> + 'a> {
        let records = proto::Transaction::read_many(reader);
        let transactions = records.map(|result| {
            let record = result?;
            record.to_transaction()
        });
        Box::new(transactions)
    }

    /// Returns transaction metadata.
    pub fn meta(&self) -> &Meta {
        match self {
            Transaction::Deposit { meta: m, .. } => m,
            Transaction::Withdrawal { meta: m, .. } => m,
            Transaction::Dispute { meta: m, .. } => m,
            Transaction::Resolve { meta: m, .. } => m,
            Transaction::Chargeback { meta: m, .. } => m,
        }
    }
}

/// Client Account model.
#[derive(Debug, Clone)]
pub struct Account {
    available_funds: Decimal,
    held_funds: Decimal,
    is_locked: bool,
}

impl Account {
    /// Creates new unlocked account with zero funds.
    pub fn new() -> Account {
        Account {
            available_funds: Decimal::ZERO,
            held_funds: Decimal::ZERO,
            is_locked: false,
        }
    }

    pub fn is_frozen(&self) -> bool {
        self.is_locked
    }

    /// Returns available funds.
    pub fn get_available_funds(&self) -> &Decimal {
        &self.available_funds
    }

    /// Deposits the given `amount` to the account.
    pub fn deposit(&mut self, amount: &Decimal) {
        self.available_funds += amount;
    }

    /// Withdraws the given `amount` from the account.
    pub fn withdraw(&mut self, amount: &Decimal) {
        assert!(
            self.available_funds >= *amount,
            "Attempting to withdraw more than the account has"
        );
        self.available_funds -= amount;
    }

    /// Holds the specified fund amount.
    pub fn hold_funds(&mut self, amount: &Decimal) {
        self.available_funds -= amount;
        self.held_funds += amount;
    }

    /// Release the previously held specified fund amount.
    pub fn release_funds(&mut self, amount: &Decimal) {
        self.available_funds += amount;
        self.held_funds -= amount;
    }

    /// Charges the previously held specified fund amount again and lock the account.
    pub fn chargeback(&mut self, amount: &Decimal) {
        self.held_funds -= amount;
        self.is_locked = true;
    }

    /// Converts account to a proto representation.
    pub fn to_proto(&self, client_id: &ClientId) -> proto::Account {
        proto::Account {
            client_id: client_id.0,
            available_funds: self.available_funds,
            held_funds: self.held_funds,
            total_funds: self.available_funds + self.held_funds,
            is_locked: self.is_locked,
        }
    }
}

/// A named pair of an item with an id. A container to pass the pair around.
#[derive(Debug)]
pub struct Record<T, U> {
    pub item: T,
    pub id: U,
}

impl<T, U> Record<T, U> {
    pub fn new(item: T, id: U) -> Record<T, U> {
        Record { item: item, id: id }
    }
}
