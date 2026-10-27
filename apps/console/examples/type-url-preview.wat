;; Preview ABI v1 example: display the input Any's type URL.
;; The host supplies a protobuf Any with type_url first, followed by value,
;; and a separately encoded FileDescriptorSet. No imports except bounded memory.
(module
  (import "env" "memory" (memory 16 256))
  (global $heap (mut i32) (i32.const 1024))
  (func (export "pm_preview_abi") (result i32) i32.const 1)
  (func (export "pm_alloc") (param $length i32) (result i32)
    (local $old i32) (local $end i32) (local $needed i32)
    global.get $heap local.tee $old
    local.get $length i32.add local.tee $end
    i32.const 65535 i32.add i32.const 16 i32.shr_u local.set $needed
    local.get $needed memory.size i32.gt_u
    if
      local.get $needed memory.size i32.sub memory.grow
      i32.const -1 i32.eq if unreachable end
    end
    local.get $end global.set $heap
    local.get $old)
  (func (export "pm_preview") (param $payload i32) (param $length i32)
    (param $schema i32) (param $schemaLength i32) (result i64)
    (local $pos i32) (local $size i32) (local $shift i32) (local $byte i32)
    local.get $length i32.const 2 i32.lt_u if unreachable end
    local.get $payload i32.load8_u i32.const 10 i32.ne if unreachable end
    local.get $payload i32.const 1 i32.add local.set $pos
    block $done
      loop $varint
        local.get $pos local.get $payload i32.sub local.get $length i32.ge_u if unreachable end
        local.get $shift i32.const 28 i32.gt_u if unreachable end
        local.get $pos i32.load8_u local.set $byte
        local.get $size local.get $byte i32.const 127 i32.and local.get $shift i32.shl i32.or local.set $size
        local.get $pos i32.const 1 i32.add local.set $pos
        local.get $byte i32.const 128 i32.and i32.eqz br_if $done
        local.get $shift i32.const 7 i32.add local.set $shift
        br $varint
      end
    end
    ;; Any.type_url and StringValue.value are both length-delimited field 1.
    local.get $pos local.get $payload i32.sub local.get $size i32.add local.set $size
    local.get $size local.get $length i32.gt_u if unreachable end
    local.get $payload i64.extend_i32_u i64.const 32 i64.shl
    local.get $size i64.extend_i32_u i64.or)
)
