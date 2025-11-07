import { useEffect, useState } from 'react'
import './App.css'
import { getSummary, getList, refresh, search, type MediaItem } from './api'

type Tab = 'analytics' | 'movies' | 'tv' | 'search'
type View = 'app' | 'auth' | 'login' | 'signup'

function Stat({label, value}:{label:string;value:React.ReactNode}){
  return (
    <div className="stat">
      <div className="stat-value">{value}</div>
      <div className="stat-label">{label}</div>
    </div>
  )
}

function Card({item}:{item:MediaItem}){
  const img = item.poster_path ? `https://image.tmdb.org/t/p/w300${item.poster_path}` : undefined
  return (
    <div className="card">
      {img ? <img src={img} alt={item.title} loading="lazy"/> : <div className="noimg">No image</div>}
      <div className="card-body">
        <div className="chip">{item.media_type.toUpperCase()}</div>
        <h4 title={item.title}>{item.title}</h4>
        <div className="meta">
          <span>⭐ {item.vote_average?.toFixed?.(1) ?? '–'}</span>
          <span>🌐 {item.original_language?.toUpperCase?.() ?? '—'}</span>
          <span>📅 {item.release_date ?? '—'}</span>
        </div>
      </div>
    </div>
  )
}

export default function App() {
  const [tab, setTab] = useState<Tab>('analytics')
  const [view, setView] = useState<View>('app')
  const [busy, setBusy] = useState(false)
  const [summary, setSummary] = useState<any>()
  const [movies, setMovies] = useState<MediaItem[]>([])
  const [tv, setTv] = useState<MediaItem[]>([])
  const [q, setQ] = useState('')
  const [results, setResults] = useState<MediaItem[]>([])
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')

  useEffect(() => { load() }, [])

  async function load(){
    setBusy(true)
    try{
      const [s, m, t] = await Promise.all([
        getSummary().catch(()=>null),
        getList('movie').then(d=>d.results).catch(()=>[]),
        getList('tv').then(d=>d.results).catch(()=>[]),
      ])
      setSummary(s)
      setMovies(m)
      setTv(t)
    } finally {
      setBusy(false)
    }
  }

  async function onRefresh(){
    setBusy(true)
    try{ await refresh(1); await load() } finally { setBusy(false) }
  }

  async function onSearch(ev: React.FormEvent){
    ev.preventDefault()
    if(!q.trim()) return
    setBusy(true)
    try{ const d = await search(q.trim()); setResults(d.results) } finally { setBusy(false) }
    setTab('search')
  }

  if(view === 'login'){
    const onSubmit = (ev: React.FormEvent) => {
      ev.preventDefault()
      // TODO: hook to backend auth
      alert(`Email: ${email}\nPassword: ${'*'.repeat(password.length)}`)
    }
    return (
      <div className="container auth-container">
        <h1 className="form-title">Log In</h1>
        <form className="auth-form" onSubmit={onSubmit}>
          <label className="form-label">Email</label>
          <input className="form-input" type="email" placeholder="name@example.com" value={email} onChange={e=>setEmail(e.target.value)} required/>

          <label className="form-label">Password</label>
          <input className="form-input" type="password" placeholder="•••••" value={password} onChange={e=>setPassword(e.target.value)} required/>

          <button className="btn-solid btn-lg" type="submit">Submit</button>
          <button className="btn-outline btn-lg" type="button" onClick={()=>{/* TODO: show stored accounts */}}>View Stored Accounts</button>
        </form>
        <div className="form-footer">
          <button className="btn-link" onClick={()=>setView('auth')}>Back</button>
        </div>
      </div>
    )
  }

  if(view === 'signup'){
    const onSubmit = (ev: React.FormEvent) => {
      ev.preventDefault()
      // TODO: hook to backend signup
      alert(`Create account for: ${email}`)
    }
    return (
      <div className="container auth-container">
        <h1 className="form-title">Sign Up</h1>
        <form className="auth-form" onSubmit={onSubmit}>
          <label className="form-label">Email</label>
          <input className="form-input" type="email" placeholder="email@example.com" value={email} onChange={e=>setEmail(e.target.value)} required/>

          <label className="form-label">Password</label>
          <input className="form-input" type="password" placeholder="password" value={password} onChange={e=>setPassword(e.target.value)} required/>

          <button className="btn-solid btn-lg" type="submit">Create Account</button>
          <button className="btn-outline btn-lg" type="button" onClick={()=>{/* TODO: show stored accounts */}}>View Stored Accounts</button>
        </form>
        <div className="form-footer">
          <button className="btn-link" onClick={()=>setView('auth')}>Back</button>
        </div>
      </div>
    )
  }

  // Auth landing page (Log in / Sign Up)
  if(view === 'auth'){
    return (
      <div className="container auth-container">
        <div className="auth-header"><button className="btn-link" onClick={()=>setView('app')}>← Back</button></div>
        <div className="auth-stack">
          <div className="auth-box auth-box-title">
            <h1>Movie &amp; TV Analytics</h1>
          </div>
          <div className="auth-box auth-box-desc">
            <p>Movie &amp; TV Analytics this is a website that shows data<br/>on Movies and TV shows</p>
          </div>
          <div className="auth-actions">
            <button className="btn-outline btn-lg" onClick={()=>setView('login')}>Log In</button>
            <button className="btn-outline btn-lg" onClick={()=>setView('signup')}>Sign Up</button>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="container">
      <header className="header">
        <div className="brand">Movie & TV Analytics</div>
        <nav className="nav">
          <button className={tab==='analytics'? 'active':''} onClick={()=>setTab('analytics')}>Analytics</button>
          <button className={tab==='movies'? 'active':''} onClick={()=>setTab('movies')}>Movies</button>
          <button className={tab==='tv'? 'active':''} onClick={()=>setTab('tv')}>TV</button>
        </nav>
        <form className="search" onSubmit={onSearch}>
          <input placeholder="Search TMDb…" value={q} onChange={e=>setQ(e.target.value)} />
          <button type="submit">Search</button>
          <button type="button" onClick={()=>setView('auth')}>Log in</button>
        </form>
      </header>

      <div className="toolbar">
        <button onClick={onRefresh} disabled={busy}>↻ Refresh (ingest 1 page)</button>
        <span className="hint">Set TMDB_API_KEY in backend .env to enable refresh/search.</span>
      </div>

      {tab==='analytics' && (
        <section>
          <div className="stats">
            <Stat label="Items" value={summary?.total_items ?? '–'} />
            <Stat label="Movies" value={summary?.movies ?? '–'} />
            <Stat label="TV" value={summary?.tv ?? '–'} />
            <Stat label="Avg Rating" value={(summary?.avg_rating ?? 0).toFixed(1)} />
          </div>

          <h3>Top Genres</h3>
          <div className="bars">
            {(summary?.top_genres ?? []).map((g:any)=> (
              <div className="bar" key={g.genre}>
                <div className="bar-label">{g.genre || '—'}</div>
                <div className="bar-track"><div style={{width: `${Math.min(100, g.count)}%`}}/></div>
                <div className="bar-value">{g.count}</div>
              </div>
            ))}
          </div>

          <h3>Languages</h3>
          <div className="chips">
            {(summary?.languages ?? []).map((l:any)=> (
              <span className="chip" key={l.language}>{l.language.toUpperCase()} · {l.count}</span>
            ))}
          </div>
        </section>
      )}

      {tab==='movies' && (
        <section>
          <h3>Top Movies</h3>
          <div className="grid">{movies.map(m => <Card key={`m-${m.tmdb_id}`} item={m} />)}</div>
        </section>
      )}

      {tab==='tv' && (
        <section>
          <h3>Top TV</h3>
          <div className="grid">{tv.map(m => <Card key={`t-${m.tmdb_id}`} item={m} />)}</div>
        </section>
      )}

      {tab==='search' && (
        <section>
          <h3>Search Results</h3>
          <div className="grid">{results.map(m => <Card key={`s-${m.media_type}-${m.tmdb_id}`} item={m} />)}</div>
        </section>
      )}

      {busy && <div className="busy">Loading…</div>}
    </div>
  )
}
