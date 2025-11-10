import { useEffect, useMemo, useState } from 'react'
import './App.css'
import { getSummary, getList, refresh, search, type MediaItem, getUsers, type UserRow, getHealth, login, signup, getTrending, type TrendingItem } from './api'

type Tab = 'home' | 'analytics' | 'movies' | 'tv' | 'search'
type View = 'app' | 'auth' | 'login' | 'signup' | 'accounts'
type TrendingPeriod = 'weekly' | 'monthly' | 'all'

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
  const [tab, setTab] = useState<Tab>('home')
  const [view, setView] = useState<View>('app')
  const [busy, setBusy] = useState(false)
  const [summary, setSummary] = useState<any>()
  const [movies, setMovies] = useState<MediaItem[]>([])
  const [tv, setTv] = useState<MediaItem[]>([])
  const [q, setQ] = useState('')
  const [results, setResults] = useState<MediaItem[]>([])
  const [username, setUsername] = useState('')
  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [accounts, setAccounts] = useState<UserRow[]>([])
  const [accountsError, setAccountsError] = useState<string | null>(null)
  const [backendOnline, setBackendOnline] = useState<boolean | null>(null)
  const [trendingPeriod, setTrendingPeriod] = useState<TrendingPeriod>('weekly')
  const [trending, setTrending] = useState<TrendingItem[]>([])
  const [trendingLoading, setTrendingLoading] = useState(false)
  const [trendingError, setTrendingError] = useState<string | null>(null)
  const [carouselIndex, setCarouselIndex] = useState(0)
  // Carousel is always locked to weekly trending
  const [carouselSlides, setCarouselSlides] = useState<TrendingItem[]>([])
  const [carouselLoading, setCarouselLoading] = useState(false)
  // Remember-me support: if a profile was stored and flag set, restore it.
  const [currentUser, setCurrentUser] = useState<{user:string;email:string}|null>(() => {
    try {
      const flag = localStorage.getItem('rememberUser') === '1'
      if(!flag) return null
      const raw = localStorage.getItem('currentUser')
      return raw? JSON.parse(raw) : null
    } catch { return null }
  })
  const [remember, setRemember] = useState<boolean>(() => localStorage.getItem('rememberUser') === '1')
  const [loginError, setLoginError] = useState<string | null>(null)
  const [signupError, setSignupError] = useState<string | null>(null)

  // Always prefill Admin creds when opening the login view
  useEffect(() => {
    if(view === 'login'){
      setEmail('Admin@Test.com')
      setPassword('Admin')
      setLoginError(null)
    }
    if(view === 'signup'){
      // Clear signup fields by default
      setUsername('')
      setEmail('')
      setPassword('')
    }
  }, [view])

  useEffect(() => { load() }, [])

  // Load carousel data once (always weekly, top 5)
  useEffect(() => {
    let cancelled = false
    setCarouselLoading(true)
    ;(async () => {
      try {
        const results = await getTrending('weekly', 5)
        if (!cancelled) {
          setCarouselSlides(results)
          setCarouselIndex(0)
        }
      } catch (err) {
        if (!cancelled) {
          setCarouselSlides([])
        }
      } finally {
        if (!cancelled) {
          setCarouselLoading(false)
        }
      }
    })()
    return () => {
      cancelled = true
    }
  }, [])

  // Load right-rail trending based on selected period
  useEffect(() => {
    let cancelled = false
    setTrendingLoading(true)
    setTrendingError(null)
    ;(async () => {
      try {
        const results = await getTrending(trendingPeriod, 40)
        if (!cancelled) {
          setTrending(results)
        }
      } catch (err) {
        if (!cancelled) {
          setTrending([])
          setTrendingError('Trending data unavailable. Try running the TMDb ETL loader.')
        }
      } finally {
        if (!cancelled) {
          setTrendingLoading(false)
        }
      }
    })()
    return () => {
      cancelled = true
    }
  }, [trendingPeriod])

  const heroSlides = useMemo(
    () => carouselSlides.filter(item => item.backdrop_url || item.poster_url),
    [carouselSlides]
  )
  const activeHeroIndex = heroSlides.length > 0 ? Math.min(carouselIndex, heroSlides.length - 1) : 0

  useEffect(() => {
    if(heroSlides.length === 0){
      setCarouselIndex(0)
      return
    }
    setCarouselIndex(prev => prev % heroSlides.length)
  }, [heroSlides.length])

  useEffect(() => {
    if(view !== 'app' || tab !== 'home') return
    if(heroSlides.length <= 1) return
    const handle = window.setInterval(() => {
      setCarouselIndex(prev => (prev + 1) % heroSlides.length)
    }, 6000)
    return () => window.clearInterval(handle)
  }, [view, tab, heroSlides.length])

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

  // Lazy-load accounts only when the view is opened
  useEffect(() => {
    if(view === 'accounts'){
      (async () => {
        try {
          setAccountsError(null)
          // health first to drive status chip
          const health = await getHealth().catch(()=>null)
          setBackendOnline(health?.status === 'healthy')
          const rows = await getUsers()
          setAccounts(rows)
        } catch (e) {
          setAccounts([])
          setAccountsError('Could not load accounts. Ensure the backend is running and the dev proxy is active.')
          setBackendOnline(false)
        }
      })()
    }
  }, [view])

  if(view === 'login'){
    const onSubmit = async (ev: React.FormEvent) => {
      ev.preventDefault()
      setLoginError(null)
      try{
        // Preflight: if backend is offline, fail fast with a clear message
        const health = await getHealth().catch(()=>null)
        if(!health || health.status !== 'healthy'){
          setLoginError('Backend is offline. Please start the backend and try again.')
          return
        }
        const res = await login(email.trim(), password)
        if(res.ok){
          const profile = { user: (res as any).user, email: (res as any).email }
          setCurrentUser(profile)
          if(remember){
            try {
              localStorage.setItem('currentUser', JSON.stringify(profile))
              localStorage.setItem('rememberUser', '1')
            } catch {}
          } else {
            try {
              localStorage.removeItem('currentUser')
              localStorage.removeItem('rememberUser')
            } catch {}
          }
          setView('app')
          setTab('home')
        } else {
          setLoginError(res.error || 'Invalid credentials')
        }
      }catch(e:any){
        setLoginError(e?.message || 'Unexpected login error')
      }
    }
    return (
      <div className="auth-page-wrapper">
        <div className="auth-background-decoration">
          <div className="auth-gradient-overlay"></div>
        </div>
        <div className="container auth-container">
          <div className="auth-card">
            <div className="auth-card-header">
              <div className="auth-icon">🎬</div>
              <h1 className="auth-card-title">Welcome Back</h1>
              <p className="auth-card-subtitle">Sign in to continue exploring movies and TV shows</p>
            </div>
            
            <form className="auth-form" onSubmit={onSubmit}>
              {loginError && (
                <div className="auth-error-message">
                  <span className="error-icon">⚠️</span>
                  <span>{loginError}</span>
                </div>
              )}
              
              <div className="form-group">
                <label className="form-label">
                  <span className="label-icon">📧</span>
                  Email Address
                </label>
                <input 
                  className="form-input" 
                  type="email" 
                  placeholder="Enter your email" 
                  value={email} 
                  onChange={e=>setEmail(e.target.value)} 
                  required
                />
              </div>

              <div className="form-group">
                <label className="form-label">
                  <span className="label-icon">🔒</span>
                  Password
                </label>
                <input 
                  className="form-input" 
                  type="password" 
                  placeholder="Enter your password" 
                  value={password} 
                  onChange={e=>setPassword(e.target.value)} 
                  required
                />
              </div>

              <div className="form-options">
                <label className="checkbox-label">
                  <input 
                    type="checkbox" 
                    checked={remember} 
                    onChange={e=>setRemember(e.target.checked)} 
                    className="checkbox-input"
                  />
                  <span>Remember me</span>
                </label>
              </div>

              <button className="btn-primary btn-submit" type="submit">
                <span>Sign In</span>
                <span className="btn-arrow">→</span>
              </button>
              
              <div className="auth-divider">
                <span>or</span>
              </div>
              
              <button 
                className="btn-secondary" 
                type="button" 
                onClick={()=> setView('accounts')}
              >
                View Stored Accounts
              </button>
            </form>
            
             <div className="auth-card-footer">
               <p>Don't have an account? <button className="link-button" onClick={()=>setView('signup')}>Sign up</button></p>
               <button className="back-link-button" onClick={()=>setView('app')}>← Back to app</button>
             </div>
          </div>
        </div>
      </div>
    )
  }

  if(view === 'signup'){
    const onSubmit = (ev: React.FormEvent) => {
      ev.preventDefault()
      setSignupError(null)
      ;(async () => {
        try {
          // Preflight: if backend is offline, fail fast with a clear message
          const health = await getHealth().catch(()=>null)
          if(!health || health.status !== 'healthy'){
            setSignupError('Backend is offline. Please start the backend and try again.')
            return
          }

          const e = email.trim()
          const p = password
          if(!e || !p){
            setSignupError('Please provide both email and password')
            return
          }

          const res = await signup(e, p, username.trim())
          if((res as any).ok){
            // After creating, go to login with email prefilled; keep password empty
            setEmail((res as any).email)
            setPassword('')
            setView('login')
          } else {
            setSignupError((res as any).error || 'Could not create account')
          }
        } catch(e:any){
          setSignupError(e?.message || 'Unexpected error during signup')
        }
      })()
    }
    return (
      <div className="auth-page-wrapper">
        <div className="auth-background-decoration">
          <div className="auth-gradient-overlay"></div>
        </div>
        <div className="container auth-container">
          <div className="auth-card">
            <div className="auth-card-header">
              <div className="auth-icon">🎭</div>
              <h1 className="auth-card-title">Join the Experience</h1>
              <p className="auth-card-subtitle">Create your account to start exploring movie and TV analytics</p>
            </div>
            
            <form className="auth-form" onSubmit={onSubmit}>
              {signupError && (
                <div className="auth-error-message">
                  <span className="error-icon">⚠️</span>
                  <span>{signupError}</span>
                </div>
              )}
              
              <div className="form-group">
                <label className="form-label">
                  <span className="label-icon">👤</span>
                  Username
                </label>
                <input 
                  className="form-input" 
                  type="text" 
                  placeholder="Choose a username" 
                  value={username} 
                  onChange={e=>setUsername(e.target.value)} 
                  required
                />
              </div>

              <div className="form-group">
                <label className="form-label">
                  <span className="label-icon">📧</span>
                  Email Address
                </label>
                <input 
                  className="form-input" 
                  type="email" 
                  placeholder="Enter your email" 
                  value={email} 
                  onChange={e=>setEmail(e.target.value)} 
                  required
                />
              </div>

              <div className="form-group">
                <label className="form-label">
                  <span className="label-icon">🔒</span>
                  Password
                </label>
                <input 
                  className="form-input" 
                  type="password" 
                  placeholder="Create a password" 
                  value={password} 
                  onChange={e=>setPassword(e.target.value)} 
                  required
                />
              </div>

              <button className="btn-primary btn-submit" type="submit">
                <span>Create Account</span>
                <span className="btn-arrow">→</span>
              </button>
              
              <div className="auth-divider">
                <span>or</span>
              </div>
              
              <button 
                className="btn-secondary" 
                type="button" 
                onClick={()=> setView('accounts')}
              >
                View Stored Accounts
              </button>
            </form>
            
             <div className="auth-card-footer">
               <p>Already have an account? <button className="link-button" onClick={()=> { 
                 setCurrentUser(null);
                 try { localStorage.removeItem('currentUser'); localStorage.removeItem('rememberUser') } catch {}
                 setView('login');
               }}>Sign in</button></p>
               <button className="back-link-button" onClick={()=>setView('app')}>← Back to app</button>
             </div>
          </div>
        </div>
      </div>
    )
  }

  // Auth landing page (Log in / Sign Up)
  const posterFor = (path?: string | null, size: 'w185' | 'w342' | 'w500' | 'w780' = 'w185') => {
    if(!path) return undefined
    if(path.startsWith('http')) return path
    const normalized = path.startsWith('/') ? path : `/${path}`
    return `https://image.tmdb.org/t/p/${size}${normalized}`
  }

  const backdropFor = (path?: string | null) => {
    return posterFor(path, 'w780')
  }

  if(view === 'auth'){
    return (
      <div className="auth-page-wrapper">
        <div className="auth-background-decoration">
          <div className="auth-gradient-overlay"></div>
        </div>
        <div className="auth-landing-content">
          <button className="back-link-button-top" onClick={()=>setView('app')}>← Back to App</button>
          
          <div className="auth-landing-header">
            <div className="auth-landing-icon">🎬</div>
            <h1 className="auth-landing-title">Movie &amp; TV Analytics</h1>
            <p className="auth-landing-tagline">Discover insights, track trends, and explore the world of entertainment</p>
          </div>

          <div className="auth-landing-features">
            <div className="auth-feature">
              <div className="auth-feature-icon">📊</div>
              <h3>Analytics Dashboard</h3>
              <p>View comprehensive statistics and trends</p>
            </div>
            <div className="auth-feature">
              <div className="auth-feature-icon">🎥</div>
              <h3>Movie Database</h3>
              <p>Explore thousands of movies and TV shows</p>
            </div>
            <div className="auth-feature">
              <div className="auth-feature-icon">🔍</div>
              <h3>Smart Search</h3>
              <p>Find your favorite content instantly</p>
            </div>
          </div>

          <div className="auth-landing-actions">
            <button className="btn-primary btn-landing" onClick={()=>setView('login')}>
              <span>Sign In</span>
              <span className="btn-arrow">→</span>
            </button>
            <button className="btn-secondary btn-landing" onClick={()=>setView('signup')}>
              Create Account
            </button>
          </div>
        </div>
      </div>
    )
  }

  if(view === 'accounts'){
    return (
      <div className="container auth-container">
        <h1 className="form-title">Stored Accounts</h1>
        <div style={{display:'flex', gap:12, alignItems:'center', marginBottom:12}}>
          <span className="chip" style={{background: backendOnline? '#2e7d32':'#b00020', color:'#fff'}}>
            Backend: {backendOnline===null? '—' : backendOnline? 'Online' : 'Offline'}
          </span>
          <button className="btn-outline" onClick={()=> setView('accounts')}>Reload</button>
        </div>
        <div style={{overflowX:'auto', width:'100%'}}>
          <table style={{width:'100%', borderCollapse:'collapse'}}>
            <thead>
              <tr>
                <th style={{textAlign:'left', borderBottom:'1px solid #ddd', padding:'8px'}}>User</th>
                <th style={{textAlign:'left', borderBottom:'1px solid #ddd', padding:'8px'}}>Email</th>
                <th style={{textAlign:'left', borderBottom:'1px solid #ddd', padding:'8px'}}>Password</th>
              </tr>
            </thead>
            <tbody>
              {accountsError ? (
                <tr>
                  <td colSpan={3} style={{padding:'12px', color:'#b00020'}}>{accountsError}</td>
                </tr>
              ) : accounts.length === 0 ? (
                <tr>
                  <td colSpan={3} style={{padding:'12px'}}>No accounts found.</td>
                </tr>
              ) : accounts.map((u, idx) => (
                <tr key={idx}>
                  <td style={{borderBottom:'1px solid #f0f0f0', padding:'8px'}}>{u.user}</td>
                  <td style={{borderBottom:'1px solid #f0f0f0', padding:'8px'}}>{u.email}</td>
                  <td style={{borderBottom:'1px solid #f0f0f0', padding:'8px', fontFamily:'monospace', fontSize:'11px'}}>{u.password}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <div className="form-footer" style={{marginTop:'16px'}}>
          <button className="btn-link" onClick={()=>setView('login')}>Back</button>
        </div>
        <p style={{fontSize:'12px', color:'#666', marginTop:'8px'}}>Passwords are stored hashed for security and shown here as stored values.</p>
      </div>
    )
  }

  return (
    <div className="container">
      <header className="header">
        <div className="brand"><span>Movie & TV Analytics</span>{currentUser && <span className="chip user-chip-blue brand-user-chip">User: {currentUser.user}</span>}</div>
        <nav className="nav">
          <div className="nav-left">
            <button className={tab==='home'? 'active':''} onClick={()=>setTab('home')}>Home</button>
          </div>
          <button className={tab==='analytics'? 'active':''} onClick={()=>setTab('analytics')}>Analytics</button>
          <button className={tab==='movies'? 'active':''} onClick={()=>setTab('movies')}>Movies</button>
          <button className={tab==='tv'? 'active':''} onClick={()=>setTab('tv')}>TV</button>
        </nav>
        <form className="search" onSubmit={onSearch}>
          <input placeholder="Search TMDb…" value={q} onChange={e=>setQ(e.target.value)} />
          <button type="submit">Search</button>
          {!currentUser ? (
            <>
              <button type="button" onClick={()=>setView('login')}>Log In</button>
              <button type="button" onClick={()=>setView('signup')}>Sign Up</button>
            </>
          ) : (
            <button
              type="button"
              onClick={()=>{ 
                setCurrentUser(null);
                try { localStorage.removeItem('currentUser'); localStorage.removeItem('rememberUser') } catch {}
              }}
            >Log Out</button>
          )}
        </form>
      </header>
      {tab==='home' && (
        <section className="hero">
          <h1>Movie &amp; TV Analytics</h1>
          <p>Track trends, analyze ratings, and discover insights across your favorite movies and shows.</p>
          <div className="hero-actions">
            <button className="btn-solid btn-lg" onClick={()=>setTab('analytics')}>Get Started</button>
          </div>

          <div className="features">
            <div className="feature-card">
              <div className="feature-icon">🎬</div>
              <h3>Explore Movies</h3>
              <p>Dive into box office stats, critic reviews, and audience trends for every film.</p>
            </div>
            <div className="feature-card">
              <div className="feature-icon">📺</div>
              <h3>Track TV Shows</h3>
              <p>Analyze popularity over seasons, compare networks, and follow ratings evolution.</p>
            </div>
            <div className="feature-card">
              <div className="feature-icon">📈</div>
              <h3>View Analytics</h3>
              <p>Visualize data trends, compare genres, and uncover audience preferences.</p>
            </div>
          </div>

          <div className="home-spotlight">
            <div className="trending-hero">
              {carouselLoading ? (
                <div className="trending-hero-empty">Loading trending titles…</div>
              ) : heroSlides.length === 0 ? (
                <div className="trending-hero-empty">
                  Run the TMDb ETL loader to populate trending titles.
                </div>
              ) : (
                heroSlides.map((item, idx) => {
                  const isActive = idx === activeHeroIndex
                  const backdrop = backdropFor(item.backdrop_url ?? item.poster_url ?? null)
                  const overview =
                    item.overview && item.overview.length > 220
                      ? `${item.overview.slice(0, 217)}…`
                      : item.overview
                  const rating = item.tmdb_vote_avg
                  return (
                    <div
                      key={`hero-${item.media_type}-${item.tmdb_id}-${idx}`}
                      className={`trending-hero-slide ${isActive ? 'active' : ''}`}
                    >
                      <div
                        className="trending-hero-backdrop"
                        style={backdrop ? { backgroundImage: `url(${backdrop})` } : undefined}
                      />
                      <div className="trending-hero-overlay" />
                      <div className="trending-hero-content">
                        <div className="trending-hero-pill">{item.media_type === 'movie' ? 'Movie' : 'TV Series'}</div>
                        <h2>{item.title}</h2>
                        <p>{overview || 'No synopsis available just yet.'}</p>
                        <div className="trending-hero-meta">
                          <span>⭐ {rating != null ? rating.toFixed(1) : '—'}</span>
                          {item.release_date && <span>📅 {item.release_date}</span>}
                          {item.genres.length > 0 && <span>{item.genres.slice(0, 2).join(' • ')}</span>}
                        </div>
                      </div>
                    </div>
                  )
                })
              )}

              {heroSlides.length > 1 && (
                <>
                  <button
                    type="button"
                    className="trending-hero-nav prev"
                    onClick={() => setCarouselIndex(prev => (prev - 1 + heroSlides.length) % heroSlides.length)}
                    aria-label="Previous slide"
                  >
                    ‹
                  </button>
                  <button
                    type="button"
                    className="trending-hero-nav next"
                    onClick={() => setCarouselIndex(prev => (prev + 1) % heroSlides.length)}
                    aria-label="Next slide"
                  >
                    ›
                  </button>
                  <div className="trending-hero-dots">
                    {heroSlides.map((_, idx) => (
                      <button
                        type="button"
                        key={`dot-${idx}`}
                        aria-label={`Show slide ${idx + 1}`}
                        className={idx === activeHeroIndex ? 'active' : ''}
                        onClick={() => setCarouselIndex(idx)}
                      />
                    ))}
                  </div>
                </>
              )}
            </div>

            <aside className="trending-rail">
              <div className="trending-rail-header">
                <div>
                  <h3>Popular &amp; Trending</h3>
                  <p>Powered by the latest titles ingested from TMDb.</p>
                </div>
                <div className="trending-rail-tabs">
                  {(['weekly', 'monthly', 'all'] as TrendingPeriod[]).map(period => (
                    <button
                      type="button"
                      key={period}
                      className={period === trendingPeriod ? 'active' : ''}
                      onClick={() => setTrendingPeriod(period)}
                    >
                      {period === 'weekly' ? 'Weekly' : period === 'monthly' ? 'Monthly' : 'All Time'}
                    </button>
                  ))}
                </div>
              </div>

              <div className="trending-rail-list">
                {trendingLoading && (
                  <div className="trending-rail-empty">Loading leaderboard…</div>
                )}
                {!trendingLoading && trending.length === 0 && (
                  <div className="trending-rail-empty">
                    {trendingError ?? 'Run the TMDb ETL loader to populate trending titles.'}
                  </div>
                )}
                {!trendingLoading && trending.map((item, index) => {
                  const poster = posterFor(item.poster_url, 'w185')
                  const genres = item.genres.length > 0 ? item.genres.slice(0, 2).join(' • ') : '—'
                  return (
                    <div key={`leaderboard-${item.media_type}-${item.tmdb_id}-${index}`} className="trending-rail-item">
                      <div className="rank">{index + 1}</div>
                      <div className="thumb">
                        {poster ? <img src={poster} alt={item.title} loading="lazy" /> : <div className="thumb-placeholder">🎬</div>}
                      </div>
                      <div className="details">
                        <div className="title" title={item.title}>{item.title}</div>
                        <div className="meta">
                          <span>{item.media_type === 'movie' ? 'Movie' : 'TV'}</span>
                          <span>⭐ {item.tmdb_vote_avg != null ? item.tmdb_vote_avg.toFixed(1) : '—'}</span>
                        </div>
                        <div className="genres">{genres}</div>
                      </div>
                    </div>
                  )
                })}
              </div>
            </aside>
          </div>

          {/* Footer removed per design update */}
        </section>
      )}


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
