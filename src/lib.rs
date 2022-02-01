pub mod models;
pub mod processing;
pub mod proto;

/// Processes transactions from the `reader` and outputs the resulted
/// client account to the `writer`.
///
/// The output accounts are sorted according to their Ord trait.
pub fn process<T: std::io::Read, U: std::io::Write>(
    reader: &mut csv::Reader<T>,
    writer: &mut csv::Writer<U>,
) {
    // TODO: Log/report errors
    let transactions = models::Transaction::read_many(reader).filter_map(|r| r.ok());
    let mut processor = processing::Processor::spawn(num_cpus::get());

    for tr in transactions {
        processor.process(tr);
    }

    let accounts = processor.wait();

    let mut records: Vec<_> = accounts.iter().map(|r| r.item.to_proto(&r.id)).collect();
    records.sort();

    for record in records {
        writer.serialize(record).unwrap();
    }
    writer.flush().unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use csv::ReaderBuilder;
    use csv::WriterBuilder;
    use indoc::indoc;

    fn check(input: &str, expected_output: &str) {
        let mut reader = ReaderBuilder::new()
            .delimiter(b',')
            .from_reader(input.as_bytes());

        let mut writer = WriterBuilder::new().delimiter(b',').from_writer(vec![]);
        process(&mut reader, &mut writer);

        let output = String::from_utf8(writer.into_inner().unwrap()).unwrap();
        assert_eq!(output, expected_output);
    }

    #[test]
    fn depositing() {
        let input = indoc! {"
            type,client,tx,amount
            deposit,1,1,4.0
            withdrawal,1,5,1.5
        "};
        let output = indoc! {"
            client,available,held,total,locked
            1,2.5,0,2.5,false
        "};
        check(input, output);
    }

    #[test]
    fn dispute() {
        let input = indoc! {"
            type,client,tx,amount
            deposit,1,1,4.0
            deposit,1,2,3.0
            dispute,1,2,
        "};
        let output = indoc! {"
            client,available,held,total,locked
            1,4,3,7,false
        "};
        check(input, output);
    }

    #[test]
    fn dispute_resolved() {
        let input = indoc! {"
            type,client,tx,amount
            deposit,1,1,4.0
            deposit,1,2,3.0
            dispute,1,2,
            resolve,1,2,
        "};
        let output = indoc! {"
            client,available,held,total,locked
            1,7,0,7,false
        "};
        check(input, output);
    }

    #[test]
    fn dispute_chargeback() {
        let input = indoc! {"
            type,client,tx,amount
            deposit,1,1,4.0
            deposit,1,2,3.0
            dispute,1,2,
            chargeback,1,2,
        "};
        let output = indoc! {"
            client,available,held,total,locked
            1,4,0,4,true
        "};
        check(input, output);
    }

    #[test]
    fn no_ops_after_chargeback() {
        let input = indoc! {"
            type,client,tx,amount
            deposit,1,1,4.0
            deposit,1,2,3.0
            dispute,1,2,
            chargeback,1,2,
            deposit,1,2,100.0
        "};
        let output = indoc! {"
            client,available,held,total,locked
            1,4,0,4,true
        "};
        check(input, output);
    }

    #[test]
    fn withdrawal_nonexisting_funds() {
        let input = indoc! {"
            type,client,tx,amount
            deposit,1,1,4.0
            withdrawal,1,2,3.0
            withdrawal,1,2,3.0
        "};
        let output = indoc! {"
            client,available,held,total,locked
            1,1,0,1,false
        "};
        check(input, output);
    }

    #[test]
    fn disputing_other_client() {
        let input = indoc! {"
            type,client,tx,amount
            deposit,1,1,4.0
            deposit,2,2,3.0
            deposit,3,3,2.0
            dispute,1,2,
            dispute,1,3,
            resolve,1,2,
            chargeback,1,3,
        "};
        let output = indoc! {"
            client,available,held,total,locked
            1,4,0,4,false
            2,3,0,3,false
            3,2,0,2,false
        "};
        check(input, output);
    }
}
