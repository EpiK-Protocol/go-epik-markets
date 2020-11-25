package network

import "io"

type CountReader struct {
	io.Reader
	count int
}

func NewCountReader(r io.Reader) *CountReader {
	return &CountReader{
		Reader: r,
		count:  0,
	}
}

func (r *CountReader) Read(buf []byte) (int, error) {
	n, err := r.Reader.Read(buf)
	r.count += n
	return n, err
}

func (r *CountReader) Count() int {
	return r.count
}

type CountWriter struct {
	io.Writer
	count int
}

func NewCountWriter(w io.Writer) *CountWriter {
	return &CountWriter{
		Writer: w,
		count:  0,
	}
}

func (w *CountWriter) Write(buf []byte) (int, error) {
	n, err := w.Writer.Write(buf)
	w.count += n
	return n, err
}

func (w *CountWriter) Count() int {
	return w.count
}
