package secret

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

var bg = context.Background()

// TestDirResolve 目录解析:去尾部换行、非法引用/穿越统一 ErrNotFound、轮换免重启。
func TestDirResolve(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "app1-hmac"), []byte("s3cr3t\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	d := NewDir(dir)

	got, err := d.Resolve(bg, "app1-hmac")
	if err != nil || string(got) != "s3cr3t" {
		t.Fatalf("应去尾部换行返回密钥: %q err=%v", got, err)
	}

	// 非法引用与不存在统一 ErrNotFound(不泄露目录结构,不可穿越)。
	for _, ref := range []string{"../etc/passwd", "a/b", "..", ".", "", "no-such"} {
		if _, err := d.Resolve(bg, ref); !errors.Is(err, ErrNotFound) {
			t.Fatalf("ref=%q 应 ErrNotFound,实际: %v", ref, err)
		}
	}

	// 轮换免重启:改文件后再次解析拿到新值(反证:若缓存则失败)。
	if err := os.WriteFile(filepath.Join(dir, "app1-hmac"), []byte("rotated\r\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err = d.Resolve(bg, "app1-hmac")
	if err != nil || string(got) != "rotated" {
		t.Fatalf("轮换后应读到新值: %q err=%v", got, err)
	}
}
