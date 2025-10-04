open Core

let%expect_test "TimestampNS.of_float_seconds" =
  let open Metrics_influx.Encoder.TimestampNS in
  let open Metrics_influx.Encoder in
  let ts = of_float_seconds 1529414438.82391405 in
  Stdio.print_endline (to_string_precision Precision.Nanosecond ts);
  [%expect {| 1529414438823913984 |}]

let%expect_test "TimestampNS.to_string" =
  let open Metrics_influx.Encoder.TimestampNS in
  let open Metrics_influx.Encoder in
  let cases =
    [
      to_string_precision Precision.Nanosecond 1529349109966270847L;
      to_string_precision Precision.Microsecond 1529349109966270847L;
      to_string_precision Precision.Millisecond 1529349109966270847L;
      to_string_precision Precision.Second 1529349109966270847L;
      to_string_precision Precision.Minute 1529349109966270847L;
      to_string_precision Precision.Hour 1529349109966270847L;
    ]
  in
  List.iter cases ~f:(fun s -> Stdio.print_endline s);
  [%expect
    {|
      1529349109966270847
      1529349109966270
      1529349109966
      1529349109
      25489151
      424819 |}]

let%expect_test "Point.to_line" =
  let open Metrics_influx.Encoder.Point in
  let open Metrics_influx.Encoder in
  let cases =
    [
      to_line
        {
          name = "count";
          tags = [ ("tag1", "val1"); ("tag2", "val2") ];
          fields =
            [
              ("value", Int 100);
              ("bool", Bool true);
              ("float", Float 1.23);
              ("int", Int 123);
              ("string", String "string\\with\"quote");
            ];
          timestamp = Some 1529349109966270847L;
        };
      to_line ~precision:Precision.Second
        {
          name = "count";
          tags = [ ("tag1", "val1"); ("tag2", "val2") ];
          fields =
            [
              ("value", Int 100);
              ("bool", Bool true);
              ("integer float", Float 100.);
            ];
          timestamp = Some 1529349109966270847L;
        };
      to_line
        {
          name = "count,with space and comma";
          tags =
            [
              ("tag1_with,comma and =equal and space", "val1"); ("tag2", "val2");
            ];
          fields = [ ("value,comma", Int 100); ("bool", Bool true) ];
          timestamp = None;
        };
      to_line
        {
          name = "count";
          tags = [];
          fields = [ ("value with space", Int 100); ("bool", Bool true) ];
          timestamp = None;
        };
    ]
  in
  List.iter cases ~f:(fun s -> Stdio.print_endline s);
  [%expect
    {|
    count,tag1=val1,tag2=val2 value=100i,bool=t,float=1.23,int=123i,string="string\with"quote" 1529349109966270847
    count,tag1=val1,tag2=val2 value=100i,bool=t,integer float=100 1529349109
    count\,with\ space\ and\ comma,tag1_with\,comma\ and\ \=equal\ and\ space=val1,tag2=val2 value,comma=100i,bool=t
    count value with space=100i,bool=t
    |}]

let%expect_test "create" =
  let open Metrics_influx.Encoder.Point in
  let m =
    create "count"
      ~fields:[ ("value", Int 100) ]
      ~timestamp:(Some 1759452208435161088L)
  in
  Stdio.print_endline (to_line m);
  [%expect {| count value=100i 1759452208435161088 |}]
