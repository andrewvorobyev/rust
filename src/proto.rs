use crate::models;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::iter::Iterator;

/// Module defines proto models for IO purposes. The models defined
/// are to be used for reading/writing data from external sources.
/// They should not be used for processing directly but can be
/// converted to/from models from `models` module.

/// Transaction model for IO use.
#[derive(Deserialize, Debug)]
pub struct Transaction {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(rename = "client")]
    pub client_id: u16,
    #[serde(rename = "tx")]
    pub transaction_id: u32,
    pub amount: Option<Decimal>,
}

impl Transaction {
    /// Reads transactions from a `csv::Reader`.
    pub fn read_many<'a, T: std::io::Read>(
        reader: &'a mut csv::Reader<T>,
    ) -> Box<dyn Iterator<Item = Result<Transaction, csv::Error>> + 'a> {
        let records = reader.deserialize::<Transaction>();
        let it = records.map(|result| -> Result<Transaction, csv::Error> {
            let record = result?;
            Ok(record)
        });

        Box::new(it)
    }

    fn meta(&self) -> models::Meta {
        models::Meta {
            client_id: models::ClientId::new(self.client_id),
            transaction_id: models::TransactionId::new(self.transaction_id),
        }
    }

    /// Converts raw record into a `models::Transaction`.
    pub fn to_transaction(&self) -> Result<models::Transaction, ParseError> {
        match self.kind.as_str() {
            "deposit" => match self.amount {
                Some(a) if a > Decimal::ZERO => Ok(models::Transaction::Deposit {
                    meta: self.meta(),
                    amount: a,
                }),
                _ => Err(ParseError::NonpositiveAmount),
            },
            "withdrawal" => match self.amount {
                Some(a) if a > Decimal::ZERO => Ok(models::Transaction::Withdrawal {
                    meta: self.meta(),
                    amount: a,
                }),
                _ => Err(ParseError::NonpositiveAmount),
            },
            "dispute" => Ok(models::Transaction::Dispute { meta: self.meta() }),
            "resolve" => Ok(models::Transaction::Resolve { meta: self.meta() }),
            "chargeback" => Ok(models::Transaction::Chargeback { meta: self.meta() }),
            other => Err(ParseError::UnknownType {
                kind: other.to_string(),
            }),
        }
    }
}

/// Client Account model for IO use.
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd, Serialize)]
pub struct Account {
    #[serde(rename = "client")]
    pub client_id: u16,
    #[serde(rename = "available")]
    pub available_funds: Decimal,
    #[serde(rename = "held")]
    pub held_funds: Decimal,
    #[serde(rename = "total")]
    pub total_funds: Decimal,
    #[serde(rename = "locked")]
    pub is_locked: bool,
}

#[derive(Debug)]
pub enum ParseError {
    Csv(csv::Error),
    UnknownType { kind: String },
    NonpositiveAmount,
}

impl From<csv::Error> for ParseError {
    fn from(err: csv::Error) -> Self {
        ParseError::Csv(err)
    }
}
