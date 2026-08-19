// Package secret 定义密钥解析端口:把密钥系统引用(*_ref)解析为明文密钥。
//
// 实现:Dir(挂载目录,生产形态——K8s Secret volume / Vault agent 的统一投递面,
// 轮换由编排层更新文件完成,免重启)与 Static(env 注入,dev/测试)。
package secret

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// ErrNotFound 引用无法解析。
var ErrNotFound = errors.New("secret: 引用未找到")

// Resolver 解析密钥引用为明文。
type Resolver interface {
	Resolve(ctx context.Context, ref string) ([]byte, error)
}

// Static 用内存映射实现(ref → 明文),适合本地/测试;禁止在生产存明文。
type Static struct{ m map[string][]byte }

// NewStatic 构造静态解析器。
func NewStatic(m map[string][]byte) Static { return Static{m: m} }

func (s Static) Resolve(_ context.Context, ref string) ([]byte, error) {
	v, ok := s.m[ref]
	if !ok {
		return nil, ErrNotFound
	}
	return v, nil
}

// refPattern 引用名白名单:非法引用与"未找到"同错,不泄露目录结构、不可读目录外文件。
var refPattern = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

// Dir 用挂载目录实现:引用 ref → <dir>/<ref> 文件内容(去尾部行结束符)。
// 每次解析现读文件:编排层滚动更新密钥文件即完成轮换,无需重启进程。
type Dir struct{ dir string }

// NewDir 构造目录解析器(dir 为挂载路径,如 K8s Secret volume 挂载点)。
func NewDir(dir string) Dir { return Dir{dir: dir} }

// Resolve 实现 Resolver。
func (d Dir) Resolve(_ context.Context, ref string) ([]byte, error) {
	if !refPattern.MatchString(ref) || ref == "." || ref == ".." {
		return nil, ErrNotFound
	}
	// #nosec G304 -- ref 已通过白名单正则(无路径分隔/././..),dir 为启动期可信配置(挂载点),不可穿越。
	b, err := os.ReadFile(filepath.Join(d.dir, ref))
	if err != nil {
		return nil, ErrNotFound // 不区分不存在/权限,统一"引用未找到"
	}
	// 去尾部行结束符:编排工具写入常带结尾换行,带 \n 的 HMAC 密钥会让验签永远失败且难排查。
	return []byte(strings.TrimRight(string(b), "\r\n")), nil
}
