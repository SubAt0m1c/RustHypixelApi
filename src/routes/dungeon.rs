use std::{collections::HashMap, time::Duration};

use actix_web::{HttpResponse, Responder, error::ErrorInternalServerError, post, web::{Bytes, Data}};
use futures::{StreamExt, stream::FuturesUnordered};
use reqwest::Client;
use serde::Serialize;
use simd_json::{base::{ValueAsArray, ValueAsScalar}, derived::ValueObjectAccess, serde::from_borrowed_value, to_borrowed_value, to_owned_value};
use uuid::Uuid;

use crate::{cache::moka_cache::{MokaCache, MokaKey}, utils::fetch_owned};

#[derive(Serialize, Debug)]
pub struct DungeonInfo {
    floors_normal: simd_json::OwnedValue, // pbs
    floors_mm: simd_json::OwnedValue,
    // secrets
    // cata_exp: &'a simd_json::BorrowedValue<'a>
}

#[post("dungeons")]
async fn dungeon_info(
    body: Bytes,
    client: Data<Client>,
    cache: Data<MokaCache>,
) -> actix_web::Result<impl Responder> {
    let mut body_vec = body.to_vec();
    let parsed_uuids = from_borrowed_value::<Vec<Uuid>>(to_borrowed_value(&mut body_vec).map_err(ErrorInternalServerError)?).map_err(ErrorInternalServerError)?;
    
    let map_size = parsed_uuids.len();
    let mut futures = FuturesUnordered::new();

    for uuid in parsed_uuids {
        futures.push({
            let key = MokaKey::profile(uuid);
            let client = client.clone();
            let cache = cache.clone();

            async move {
                if let Some(cached) = cache.get(key).await {
                    return Ok::<(Uuid, Bytes), actix_web::Error>((uuid, cached))
                }

                let url = format!("https://api.hypixel.net/v2/skyblock/profiles?uuid={}", uuid);
                let res: Bytes = fetch_owned(url, client).await.map_err(ErrorInternalServerError)?;
                cache.insert(key, res.clone(), Duration::from_secs(600)).await;
                Ok::<(Uuid, Bytes), actix_web::Error>((uuid, res))
            }
        });
    }

    let mut parsed: HashMap<Uuid, DungeonInfo> = HashMap::with_capacity(map_size);

    while let Some(Ok((uuid, data))) = futures.next().await {
        let uuid = uuid;
        let mut vec: Vec<u8> = data.into();  
        let parsed_data = to_owned_value(&mut vec).map_err(ErrorInternalServerError)?;
        
        let Some(selected_profile) = parsed_data.get("profiles")
            .and_then(|profiles| profiles.as_array())
            .and_then(|a| a.iter().find(|v| v.get("selected").and_then(|selected| selected.as_bool()).unwrap_or(false)))
            .and_then(|profile| profile.get("members"))
            .and_then(|members| members.get(uuid.as_simple().to_string().as_str()))
        else { continue };

        let Some(dungeons) = selected_profile.get("dungeons")
            .and_then(|dungeons| dungeons.get("dungeon_types"))
        else { continue };

        let Some(normal) = dungeons.get("catacombs") else { continue };
        let Some(mastermode) = dungeons.get("master_catacombs") else { continue };

        let info = DungeonInfo {
            floors_mm: mastermode.clone(),
            floors_normal: normal.clone(),
        };

        parsed.insert(uuid, info);
    };

    Ok(HttpResponse::Ok().json(parsed))
}
