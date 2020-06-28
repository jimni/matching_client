use clap::App;
use config::*;
use itertools::Itertools;
use std::collections::HashMap;
use std::io;
use std::time::Instant;

fn main() -> io::Result<()> {
    let args = App::new("matching_api_client")
        .args_from_usage("-c, --config=[FILE] '!Mandatory! Sets a config file'")
        .get_matches();
    let config_file = args.value_of("config").unwrap();

    let mut settings = Config::default();
    settings.merge(File::with_name(config_file)).unwrap();

    let mpid: u16 = settings.get::<u16>("MPID").unwrap();
    let uri = settings.get_str("URI").unwrap();
    let batchsize = settings.get::<usize>("BATCHSIZE").unwrap();

    let mut debug_counter: usize = 0;
    let debug_max_iterations = settings.get::<usize>("DEBUGITERATOR").unwrap();
    let mut unmatched_msgs: usize = 0;

    let now = Instant::now();

    let mut stats = StatsTable {
        templates: HashMap::new(),
        total_msgs: 0,
    };

    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .escape(Some(b'\\'))
        .from_path(&settings.get_str("infile_path").unwrap())?;
    let mut csv_writer = csv::Writer::from_path(&settings.get_str("outfile_path").unwrap())?;

    for chunk in csv_reader.records().chunks(batchsize).into_iter() {
        if debug_counter > debug_max_iterations {
            break;
        }
        debug_counter += 1;
        if debug_counter % 10 == 0 {
            println!(
                "debug cntr = {}. Elapsed time: {}s",
                &debug_counter,
                now.elapsed().as_secs()
            );
        };
		
		// let mut messages = make_messages_vec(&mut chunk); //TODO: make it work

        let mut messages = Vec::new();
        for item in chunk {
            let record = item?;
            let msg = ShortMessage {
                sender: record.get(2).unwrap().to_string(), // TODO: make field positions configurable
                text: record.get(8).unwrap().to_string(),
                received_at: record.get(1).unwrap().to_string(),
                to: record.get(3).unwrap().to_string(),
                kind: SMKind::Advertisement,
                template_id: None,
                weight: None,
            };
            messages.push(msg);
        }

        messages = resolve_message_kind(messages, mpid, &uri);

        for msg in messages {
            match msg.template_id {
                Some(t_id) => stats.append(t_id, msg.weight.unwrap()),
                None => {
                    csv_writer.write_record(&[msg.received_at, msg.to, msg.text])?;
                    unmatched_msgs += 1;
                }
            }
        }
    }
    // stats.show();
    stats.to_csv(&settings.get_str("stats_outfile_path").unwrap());
    println!("Unmatched msgs = {}", unmatched_msgs);
    println!("Elapsed time: {}s", now.elapsed().as_secs());
    Ok(())
}

fn make_messages_vec(csv_chunk: &mut dyn Iterator<Item = csv::StringRecord>) -> Vec<ShortMessage> {
    let mut messages = Vec::new();
    for item in csv_chunk {
        let record = item;
        let msg = ShortMessage {
            sender: record.get(2).unwrap().to_string(), // TODO: make field positions configurable
            text: record.get(8).unwrap().to_string(),
            received_at: record.get(1).unwrap().to_string(),
            to: record.get(3).unwrap().to_string(),
            kind: SMKind::Advertisement,
            template_id: None,
            weight: None,
        };
        messages.push(msg);
    }
	return messages;
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
    for (i, single_match) in matches.members().enumerate() {
        // TODO: use zip instead of enumeration
        msgs[i].kind = match single_match["template_purpose"].as_str() {
            Some("transaction") => SMKind::Transaction,
            Some("service") => SMKind::Service,
            _ => SMKind::Advertisement,
        };
        msgs[i].template_id = single_match["template_id"].as_usize();
        msgs[i].weight = single_match["weight"].as_usize();
    }

    return msgs;
}

// TODO: make it thread-safe and switch to async http requests
struct StatsTable {
    templates: HashMap<usize /* template_id */, TemplateStat>,
    total_msgs: usize,
}

impl StatsTable {
    fn append(&mut self, template_id: usize, weight: usize) {
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
        self.total_msgs += 1;
    }

    fn show(&self) {
        // TODO: convert to display trait or another fmt trait
        for (k, v) in &self.templates {
            println!("{} -> {:?}", k, v);
        }
        println!("---\nTotal msgs matched = {}", self.total_msgs);
    }

    fn to_csv(&self, outpath: &String) {
        let mut stats_writer = csv::Writer::from_path(&outpath).unwrap();
        for (k, v) in &self.templates {
            stats_writer
                .write_record(&[
                    k.to_string(),
                    v.msg_count.to_string(),
                    v.msg_weight.to_string(),
                ])
                .unwrap();
        }
    }
}

#[derive(Debug)]
struct TemplateStat {
    msg_count: usize,
    msg_weight: usize,
}

#[derive(Debug)]
struct ShortMessage {
    received_at: String,
    to: String,
    sender: String,
    text: String,
    kind: SMKind,
    template_id: Option<usize>,
    weight: Option<usize>,
}

#[derive(Debug)]
enum SMKind {
    Transaction,
    Service,
    Advertisement,
}
