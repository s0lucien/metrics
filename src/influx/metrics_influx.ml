(*
 * Copyright (c) 2018 Hannes Mehnert <hannes@mehnert.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

(*************)
(* influxdb line protocol reporter *)
(* from https://docs.influxdata.com/influxdb/v1.5/write_protocols/line_protocol_reference/ *)
(* example line: weather,location=us-midwest temperature=82 1465839830100400200 *)
(*************)

open Core
open Async
module Encoder = Encoder

let encode_line_protocol tags data name =
  let metrics_fields = Metrics.Data.fields data in
  let timestamp = Option.map (Metrics.Data.timestamp data) ~f:Int64.of_string in
  let fields =
    List.map metrics_fields ~f:(fun (f : Metrics.field) ->
        let name = Metrics.key f in
        match Metrics.value f with
        | V (String, v) -> Encoder.Field.string ~name v
        | V (Bool, v) -> Encoder.Field.bool ~name v
        | V (Float, v) -> Encoder.Field.float ~name v
        | V (Int, v) -> Encoder.Field.int ~name v
        | V (Int32, v) ->
          Encoder.Field.string ~name (Fmt.to_to_string Fmt.int32 v ^ "i")
        | V (Int64, v) ->
          Encoder.Field.string ~name (Fmt.to_to_string Fmt.int64 v ^ "i")
        | V (Uint, v) ->
          Encoder.Field.string ~name (Fmt.to_to_string Fmt.uint v ^ "u")
        | V (Uint32, v) ->
          Encoder.Field.string ~name (Fmt.to_to_string Fmt.uint32 v ^ "u")
        | V (Uint64, v) ->
          Encoder.Field.string ~name (Fmt.to_to_string Fmt.uint64 v ^ "u")
        | V (Other fmt, v) ->
          Encoder.Field.string ~name (Fmt.to_to_string fmt v))
  in
  let tags =
    List.map
      (tags : Metrics.tags)
      ~f:(fun (f : Metrics.field) ->
        let name = Metrics.key f in
        match Metrics.value f with
        | V (Other fmt, v) -> (name, Fmt.to_to_string fmt v)
        | V (String, v) -> (name, v)
        | V (Bool, v) -> (name, Fmt.to_to_string Fmt.bool v)
        | V (Float, v) -> (name, Fmt.to_to_string Fmt.float v)
        | V (Int, v) -> (name, Fmt.to_to_string Fmt.int v)
        | V (Int32, v) -> (name, Fmt.to_to_string Fmt.int32 v)
        | V (Int64, v) -> (name, Fmt.to_to_string Fmt.int64 v)
        | V (Uint, v) -> (name, Fmt.to_to_string Fmt.uint v)
        | V (Uint32, v) -> (name, Fmt.to_to_string Fmt.uint32 v)
        | V (Uint64, v) -> (name, Fmt.to_to_string Fmt.uint64 v))
  in
  let encoder = Encoder.Point.create ~tags ~timestamp ~fields name in
  Encoder.Point.to_line encoder

let async_reporter ?tags:(more_tags = []) ?interval send now =
  let m = ref Map.Poly.empty in
  let i = match interval with None -> 0L | Some s -> Duration.of_ms s in
  let report ~tags ~data ~over src k =
    let send () =
      m := Map.Poly.set !m ~key:src ~data:(now ());
      let str =
        encode_line_protocol (more_tags @ tags) data (Metrics.Src.name src)
      in
      let unblock () =
        over ();
        Deferred.unit
      in
      don't_wait_for
        (Monitor.protect ~finally:(fun () -> unblock ()) (fun () -> send str));

      k ()
    in
    match Map.Poly.find !m src with
    | None -> send ()
    | Some last ->
      if Int64.(now () > last + i) then send ()
      else (
        over ();
        k ())
  in
  { Metrics.report; now; at_exit = (fun () -> ()) }
