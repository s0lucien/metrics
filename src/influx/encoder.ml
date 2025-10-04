open Base

(* Precision and TimestampNS copied from https://github.com/ryanslade/influxdb-ocaml/blob/master/influxdb/influxdb.ml *)
module Precision = struct
  type t = Nanosecond | Microsecond | Millisecond | Second | Minute | Hour

  let to_string = function
    | Nanosecond -> "ns"
    | Microsecond -> "u"
    | Millisecond -> "ms"
    | Second -> "s"
    | Minute -> "m"
    | Hour -> "h"

  let of_string = function
    | "ns" -> Some Nanosecond
    | "u" -> Some Microsecond
    | "ms" -> Some Millisecond
    | "s" -> Some Second
    | "m" -> Some Minute
    | "h" -> Some Hour
    | _ -> None
end

module TimestampNS = struct
  type t = int64

  let of_float_seconds f =
    let ns_per_sec = 1_000_000_000L in
    f *. Float.of_int64 ns_per_sec |> Int64.of_float

  let to_string_precision p t =
    let open Int64 in
    (match p with
    | Precision.Nanosecond -> t
    | Precision.Microsecond -> t / 1000L
    | Precision.Millisecond -> t / 1_000_000L
    | Precision.Second -> t / 1_000_000_000L
    | Precision.Minute -> t / 60_000_000_000L
    | Precision.Hour -> t / 3600_000_000_000L)
    |> Int64.to_string
end

(* https://docs.influxdata.com/influxdb3/core/reference/line-protocol *)

let escape_comma_space s =
  let buf = Buffer.create (String.length s) in
  String.iter s ~f:(fun c ->
      match c with
      | ' ' -> Buffer.add_string buf "\\ "
      | ',' -> Buffer.add_string buf "\\,"
      | _ -> Buffer.add_char buf c);
  Buffer.contents buf

let escape_comma_space_equal s =
  let buf = Buffer.create (String.length s) in
  String.iter s ~f:(fun c ->
      match c with
      | ' ' -> Buffer.add_string buf "\\ "
      | '=' -> Buffer.add_string buf "\\="
      | ',' -> Buffer.add_string buf "\\,"
      | _ -> Buffer.add_char buf c);
  Buffer.contents buf

let escape_doublequote_backslash s =
  let buf = Buffer.create (String.length s) in
  String.iter s ~f:(fun c ->
      match c with
      | '"' -> Buffer.add_string buf "\\\""
      | '\\' -> Buffer.add_string buf "\\\\"
      | _ -> Buffer.add_char buf c);
  Buffer.contents buf

module Field = struct
  type field_key = string

  type field_value =
    | Float of float
    (* Int should actually be Int64.t for a 1-1 mapping to influxdb*)
    | Int of int
    | String of string
    | Bool of bool

  type t = field_key * field_value

  let v_to_string = function
    | Float f -> String.rstrip ~drop:(Char.equal '.') (Float.to_string f)
    | Int i -> Printf.sprintf "%ii" i
    (* missing UInt64 *)
    | String s -> Printf.sprintf "\"%s\"" s
    | Bool b -> if b then "t" else "f"

  let to_string (k, v) = k ^ "=" ^ v_to_string v

  let float ?(name = "value") value =
    (escape_comma_space_equal name, Float value)

  let int ?(name = "value") value = (escape_comma_space_equal name, Int value)

  let string ?(name = "value") value =
    (escape_comma_space_equal name, String (escape_doublequote_backslash value))

  let bool ?(name = "value") value = (escape_comma_space_equal name, Bool value)
end

module Point = struct
  type t = {
    name : string;
    tags : (string * string) list;
    fields : Field.t list;
    (* If None, a timestamp will be assigned by InfluxDB  *)
    timestamp : TimestampNS.t option;
  }

  (* Returns the line format representation of [t] *)
  let to_line ?(precision = Precision.Nanosecond) t =
    let fields =
      List.map ~f:Field.to_string t.fields |> String.concat ~sep:","
    in
    let tags =
      match t.tags with
      | [] -> ""
      | tt ->
        ","
        ^ (List.map
             ~f:(fun (k, v) ->
               escape_comma_space_equal k ^ "=" ^ escape_comma_space_equal v)
             tt
          |> String.concat ~sep:",")
    in
    let line =
      Printf.sprintf "%s%s %s" (escape_comma_space t.name) tags fields
    in
    match t.timestamp with
    | None -> line
    | Some ts -> line ^ " " ^ TimestampNS.to_string_precision precision ts

  let create ?(tags = []) ?(timestamp = None) ~fields name =
    { name; fields; tags; timestamp }
end
