use crate::models::{Account, ClientId, Record, Transaction, TransactionId};
use rust_decimal::Decimal;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hash;
use std::hash::Hasher;
use std::iter::Iterator;
use std::rc::Rc;
use std::sync::mpsc;
use std::thread;

type Output = Vec<Record<Account, ClientId>>;

fn disputed_amount(tr: &Transaction, client_id: ClientId) -> Option<Decimal> {
    if tr.meta().client_id != client_id {
        // It transaction does not belong to the given client account - it's not disputable by this client.
        None
    } else {
        match tr {
            // We can also support disputing withdrawals later
            // and return a negative amount for that.
            Transaction::Deposit { amount: a, .. } => Some(*a),
            _ => None,
        }
    }
}

/// Partition that processes transactions sequantially.
struct Partition {
    transaction_history: HashMap<TransactionId, Rc<Transaction>>,
    disputed_transactions: HashMap<TransactionId, Rc<Transaction>>,
    pub accounts: HashMap<ClientId, Account>,
}

impl Partition {
    /// Creates a new empty partition.
    pub fn new() -> Partition {
        Partition {
            transaction_history: HashMap::new(),
            disputed_transactions: HashMap::new(),
            accounts: HashMap::new(),
        }
    }

    /// Processes the given transaction.
    ///
    /// A partition keeps the history of all transactions it has processed
    /// for handling of disputes.
    /// TODO: some prunning logic or moving history to exernal store may be required in the future.
    pub fn process(&mut self, tr: Transaction) {
        let meta = tr.meta();
        let acc = self
            .accounts
            .entry(meta.client_id)
            .or_insert_with(|| Account::new());

        if acc.is_frozen() {
            return;
        }

        match tr {
            Transaction::Deposit { amount: a, .. } => acc.deposit(&a),
            Transaction::Withdrawal { amount: a, .. } => {
                if acc.get_available_funds() >= &a {
                    acc.withdraw(&a);
                }
                // TODO: log the `else` case
            }
            Transaction::Dispute { .. } => {
                for disputed_tr in self.transaction_history.get(&meta.transaction_id) {
                    for amount in disputed_amount(disputed_tr, meta.client_id) {
                        acc.hold_funds(&amount);
                        self.disputed_transactions
                            .insert(disputed_tr.meta().transaction_id, Rc::clone(disputed_tr));
                    }
                }
            }
            Transaction::Resolve { .. } => {
                for disputed_tr in self.disputed_transactions.get(&meta.transaction_id) {
                    for amount in disputed_amount(disputed_tr, meta.client_id) {
                        acc.release_funds(&amount);
                    }
                }
            }
            Transaction::Chargeback { .. } => {
                for disputed_tr in self.disputed_transactions.get(&meta.transaction_id) {
                    for amount in disputed_amount(disputed_tr, meta.client_id) {
                        acc.chargeback(&amount);
                    }
                }
            }
        }

        self.transaction_history
            .insert(meta.transaction_id, Rc::new(tr));
    }
}

/// Worker thread command.
enum Command {
    Job(Transaction),
    Halt,
}

/// Worker thread running a single partition.
///
/// * `handle` - a thread handle.
/// * `sender` - input chanel for sending task to the worker.
struct Worker {
    handle: thread::JoinHandle<()>,
    sender: mpsc::Sender<Box<Command>>,
}

/// Transaction processor. Works by distributing transactions between
/// the workers.
///
/// Transactions are partitioned between the workers based on the client id.
/// That ensures all transactions for a single client are processed sequentially.
/// This is important to ensure no withdrawals happen before deposits, no double
/// withdrowals etc.
///
/// The processor is created by spawning it (see `spawn`)
///
/// TODO: ensure the struct constructor is private.
pub struct Processor {
    workers: Vec<Worker>,
    receiver: mpsc::Receiver<Box<Output>>,
}

impl Processor {
    /// Creates a new processor with the specified number of cores (threads).
    /// The processor spawns the treads immediately.
    pub fn spawn(n_cores: usize) -> Processor {
        let (acc_sender, acc_receiver) = mpsc::channel::<Box<Output>>();

        let workers: Vec<Worker> = (0..n_cores)
            .map(|_| {
                let (cmd_sender, cmd_receiver) = mpsc::channel::<Box<Command>>();
                let acc_sender = acc_sender.clone();

                let handle = thread::spawn(move || {
                    let mut partition = Partition::new();
                    loop {
                        let cmd = *cmd_receiver.recv().unwrap();
                        match cmd {
                            Command::Job(tr) => partition.process(tr),
                            Command::Halt => break,
                        }
                    }

                    let accs: Vec<_> = partition
                        .accounts
                        .into_iter()
                        .map(|(client_id, account)| Record::new(account, client_id))
                        .collect();
                    acc_sender.send(Box::new(accs)).unwrap();
                });

                Worker {
                    handle: handle,
                    sender: cmd_sender,
                }
            })
            .collect();

        Processor {
            workers: workers,
            receiver: acc_receiver,
        }
    }

    /// Submits transaction `tr` for processing.
    pub fn process(&self, tr: Transaction) {
        let n_workers = self.workers.len();
        assert!(n_workers > 0, "Processor is halted!");

        let mut hasher = DefaultHasher::new();
        tr.meta().client_id.hash(&mut hasher);
        let worker_id = (hasher.finish() % n_workers as u64) as usize;
        self.workers[worker_id]
            .sender
            .send(Box::new(Command::Job(tr)))
            .unwrap();
    }

    /// Waits for processor to finish running all submitted transactions.
    /// Returns the resulting account. Account order is unspecified.
    pub fn wait(&mut self) -> Output {
        let n_workers = self.workers.len();

        for worker in &self.workers {
            worker.sender.send(Box::new(Command::Halt)).unwrap();
        }

        // TODO: it should probably be possible to do it simpler
        while self.workers.len() != 0 {
            let worker = self.workers.pop().unwrap();
            worker.handle.join().unwrap();
        }

        let mut output = Output::new();
        for _ in 0..n_workers {
            for acc in self.receiver.recv().unwrap().into_iter() {
                output.push(acc);
            }
        }

        output
    }
}
