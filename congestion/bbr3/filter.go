package bbr3

type filterSample struct {
	t uint64
	v float64
}

// maxFilter is Kathleen Nichols' windowed max filter, as in Linux's lib/win_minmax.c.
type maxFilter struct {
	s [3]filterSample
}

func (m *maxFilter) get() float64 { return m.s[0].v }

func (m *maxFilter) reset(t uint64, v float64) {
	m.s[0] = filterSample{t, v}
	m.s[1] = m.s[0]
	m.s[2] = m.s[0]
}

func (m *maxFilter) update(window, t uint64, v float64) float64 {
	n := filterSample{t, v}
	if v >= m.s[0].v || t-m.s[2].t > window {
		m.reset(t, v)
		return v
	}
	if v >= m.s[1].v {
		m.s[1], m.s[2] = n, n
	} else if v >= m.s[2].v {
		m.s[2] = n
	}

	dt := t - m.s[0].t
	switch {
	case dt > window:
		m.s[0], m.s[1], m.s[2] = m.s[1], m.s[2], n
		if t-m.s[0].t > window {
			m.s[0], m.s[1], m.s[2] = m.s[1], m.s[2], n
		}
	case m.s[1].t == m.s[0].t && dt > window/4:
		m.s[1], m.s[2] = n, n
	case m.s[2].t == m.s[1].t && dt > window/2:
		m.s[2] = n
	}
	return m.s[0].v
}
