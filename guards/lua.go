package guards

import "github.com/redis/go-redis/v9"

var incrementN = redis.NewScript(`
redis.replicate_commands()
local limit_key = KEYS[1]
local rate = tonumber(ARGV[1])
local period = tonumber(ARGV[2])

local now = redis.call("TIME")[1] -- 現在時刻(秒)
local expire_at = now + period -- 有効期限時刻(秒)
local cnt = redis.call("LLEN", limit_key) -- 現在のカウント数
for i = 0, cnt do -- 古いエントリの削除
	local v = redis.call("LRANGE", limit_key, 0, 1 )
	if next(v) and v[1] < now then -- 古いエントリが存在する場合
		redis.call("LPOP", limit_key)
		cnt = cnt - 1 -- カウントをデクリメント
	else
		break -- 古いエントリが無くなったらループを抜ける
	end
end

if cnt < rate then -- カウントが制限内の場合
 	cnt = redis.call("RPUSH", limit_key, expire_at) -- 新しいエントリを追加
	redis.call("EXPIRE", limit_key, period) -- キーの有効期限を設定
	return {1, cnt, expire_at, rate, period, limit_key} -- 成功を返す
end
redis.call("EXPIRE", limit_key, period) -- キーの有効期限を設定
return {0, cnt, expire_at, rate, period, limit_key} -- 制限超過を返す
`)

var gcN = redis.NewScript(`
redis.replicate_commands()
local limit_key = KEYS[1]

local now = redis.call("TIME")[1]
local cnt = redis.call("LLEN", limit_key)
for i = 0, cnt do -- 古いエントリの削除
	local v = redis.call("LRANGE", limit_key, 0, 1 )
	if next(v) and v[1] < now then -- 古いエントリが存在する場合
		redis.call("LPOP", limit_key)
		cnt = cnt - 1 -- カウントをデクリメント
	else
		break -- 古いエントリが無くなったらループを抜ける
	end
end
return {cnt}
`)

func IncrementScript() string {
	return incrementN.Hash()
}
func GCScript() string {
	return gcN.Hash()
}
