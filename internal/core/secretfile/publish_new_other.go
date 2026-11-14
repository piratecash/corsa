//go:build !android

package secretfile

func (d *Dir) publishNew(tmp, name string) error {
	return d.root.Link(tmp, name)
}
