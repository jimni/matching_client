use itertools::Itertools;
use std::collections::HashMap;
use std::io;
use std::time::Instant;

fn main() -> io::Result<()> {
    let infile_path = "/Users/jim/Desktop/sample.csv";
    let outfile_path = "/Users/jim/Desktop/out.csv";

    let mut counter: usize = 0;
    const MPID: u16 = 2120;
    const URI: &str = "http://a2p-http-auto-vm-mf-a2p.staging.funbox.io/api/v1/match_template";
    const BATCHSIZE: usize = 1000;
    const DEBUGITER: usize = 1000;

    let now = Instant::now();

    let mut stats = StatsTable {
        templates: HashMap::new(),
    };

    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .escape(Some(b'\\'))
        .from_path(infile_path)?;
    let mut csv_writer = csv::Writer::from_path(outfile_path)?;

    for chunk in csv_reader.records().chunks(BATCHSIZE).into_iter() {
        if counter > DEBUGITER {
            break;
        }
        counter += 1;

        //TODO: convert building messages vector to function. Need to pass Iter over Chunks of StringRecords to this function. How the fuck do you annotate this
        let mut messages = Vec::new();
        for item in chunk {
            let record = item?;
            let msg = ShortMessage {
                sender: record.get(2).unwrap().to_string(),
                text: record.get(8).unwrap().to_string(),
                kind: SMKind::Advertisement,
                template_id: None,
                weight: None,
            };
            messages.push(msg);
        }

        messages = resolve_message_kind(messages, MPID, URI);

        for msg in messages {
            match msg.template_id {
                Some(t_id) => stats.append(t_id, msg.weight.unwrap()),
                None => csv_writer.write_record(&[msg.sender, msg.text])?,
            }
        }
    }
    stats.show();
	println!("{}", now.elapsed().as_secs());
    Ok(())
}

fn resolve_message_kind(mut msgs: Vec<ShortMessage>, mpid: u16, uri: &str) -> Vec<ShortMessage> {
    let mut json_data = json::JsonValue::new_array();
    for msg in &msgs {
        let json_sms = json::object! {
            mp_id: mpid,
            number: &msg.sender[..],
            text: &msg.text[..]
        };

        json_data.push(json_sms).unwrap();
    }
    let json_serialized = json::stringify(json_data);

    let client = reqwest::blocking::Client::new();
    let res = client.post(uri).body(json_serialized).send().unwrap();

    let parsed_res = json::parse(&res.text().unwrap()).unwrap();
    let matches = &parsed_res["result"]["matches"];
    for (i, single_match) in matches.members().enumerate() { // TODO: use zip instead of enumeration 
        msgs[i].kind = match single_match["template_purpose"].as_str() {
            Some("transaction") => SMKind::Transaction,
            Some("service") => SMKind::Service,
            _ => SMKind::Advertisement,
        };
        msgs[i].template_id = single_match["template_id"].as_usize();
        msgs[i].weight = single_match["weight"].as_u64();
    }

    return msgs;
}

// TODO: make it thread-safe and switch to async http requests
struct StatsTable {
    templates: HashMap<usize /* template_id */, TemplateStat>,
}

impl StatsTable {
    fn append(&mut self, template_id: usize, weight: u64) {
        match self.templates.get_mut(&template_id) {
            Some(v) => {
                v.msg_count += 1;
                v.msg_weight += weight;
            }
            None => {
                self.templates.insert(
                    template_id,
                    TemplateStat {
                        msg_count: 1,
                        msg_weight: weight,
                    },
                );
                ();
            }
        };
    }
    fn show(&self) {
        // TODO: convert to display trait or another fmt trait
        println!("{:?}", self.templates);
    }
}

#[derive(Debug)]
struct TemplateStat {
    msg_count: u64,
    msg_weight: u64,
}

#[derive(Debug)]
struct ShortMessage {
    sender: String,
    text: String,
    kind: SMKind,
    template_id: Option<usize>,
    weight: Option<u64>,
}

#[derive(Debug)]
enum SMKind {
    Transaction,
    Service,
    Advertisement,
}
