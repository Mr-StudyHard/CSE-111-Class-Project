import { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import './App.css'
import { getSummary, getList, refresh, search, type MediaItem, getUsers, type UserRow, getHealth, login, signup, getTrending, type TrendingItem, getNewReleases, getMovieDetail, getShowDetail, type MovieDetail, type ShowDetail } from './api'
type TrendingPeriod = 'weekly' | 'monthly' | 'all'
type ReleaseFilter = 'all' | 'movie' | 'tv'

const ADULT_KEYWORDS = /(erotic|erotica|adult|porn|pornographic|xxx|nsfw|hentai|ecchi|seinen|mature|r-rated|18\+|guilty pleasure)/i
const isAdultContent = (item: { media_type?: string; genres?: string[]; title?: string }) => {
  // Check genres for adult keywords
  if(item.genres?.some(g => ADULT_KEYWORDS.test(g))) return true
  // Check title for adult keywords
  if(item.title && ADULT_KEYWORDS.test(item.title)) return true
  return false
}

const NEW_RELEASE_FETCH_LIMIT = 24
const NEW_RELEASE_PAGE_SIZE = 6
const LIST_PAGE_SIZE = 24

const languageDisplayNames =
  typeof Intl !== 'undefined' && (Intl as any).DisplayNames
    ? new (Intl as any).DisplayNames(['en'], { type: 'language' })
    : null

const formatLanguageLabel = (code?: string | null) => {
  if(!code) return null
  const normalized = code.trim().toLowerCase()
  if(!normalized) return null
  try {
    const readable = (languageDisplayNames as any)?.of?.(normalized)
    if(typeof readable === 'string' && readable.trim().length > 0) {
      return readable
    }
  } catch {}
  return normalized.toUpperCase()
}

function Card({item, onClick}:{item:MediaItem; onClick?: () => void}){
  const img = item.poster_path ? `https://image.tmdb.org/t/p/w300${item.poster_path}` : undefined
  const adult = isAdultContent(item)
  return (
    <div className={`card${adult ? ' card-adult' : ''}${onClick ? ' card-clickable' : ''}`} onClick={onClick}>
      <div className="card-media">
      {img ? <img src={img} alt={item.title} loading="lazy"/> : <div className="noimg">No image</div>}
        {adult && <div className="card-adult-overlay"><span className="card-adult-badge">18+</span><span>Adult content hidden</span></div>}
      </div>
      <div className="card-body">
        <div className="chip">{item.media_type?.toUpperCase()}</div>
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

function Stat({label, value}:{label:string;value:React.ReactNode}){
  return (
    <div className="stat">
      <div className="stat-value">{value}</div>
      <div className="stat-label">{label}</div>
    </div>
  )
}

export default function App() {
  const navigate = useNavigate()
  const location = useLocation()
  
  // Initialize state from URL on first mount
  const getInitialTab = () => {
    const path = location.pathname
    if (path === '/analytics') return 'analytics'
    if (path === '/movies') return 'movies'
    if (path === '/tv') return 'tv'
    if (path === '/search') return 'search'
    return 'home'
  }
  
  const getInitialView = () => {
    const path = location.pathname
    if (path === '/login') return 'login'
    if (path === '/signup') return 'signup'
    if (path === '/accounts') return 'accounts'
    if (path.startsWith('/movie/') || path.startsWith('/show/')) return 'detail'
    return 'app'
  }
  
  const [tab, setTab] = useState<string>(getInitialTab)
  const [view, setView] = useState<string>(getInitialView)
  const [busy, setBusy] = useState(false)
  const [summary, setSummary] = useState<any>()
  const [movies, setMovies] = useState<MediaItem[]>([])
  const [moviesPage, setMoviesPage] = useState(1)
  const [moviesTotal, setMoviesTotal] = useState(0)
  const [moviesLoading, setMoviesLoading] = useState(false)
  const [tv, setTv] = useState<MediaItem[]>([])
  const [tvPage, setTvPage] = useState(1)
  const [tvTotal, setTvTotal] = useState(0)
  const [tvLoading, setTvLoading] = useState(false)
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
  const [newReleases, setNewReleases] = useState<MediaItem[]>([])
  const [newReleasesLoading, setNewReleasesLoading] = useState(false)
  const [newReleasesError, setNewReleasesError] = useState<string | null>(null)
  const [newReleaseFilter, setNewReleaseFilter] = useState<ReleaseFilter>('all')
  const [newReleasePage, setNewReleasePage] = useState(0)
  // Remember-me support: if a profile was stored and flag set, restore it.
  const [currentUser, setCurrentUser] = useState<{user:string;email:string}|null>(() => {
    try {
      const rawSession = sessionStorage.getItem('currentUser')
      if(rawSession){
        return JSON.parse(rawSession)
      }
    } catch {}
    try {
      const flag = localStorage.getItem('rememberUser') === '1'
      if(!flag) return null
      const raw = localStorage.getItem('currentUser')
      return raw? JSON.parse(raw) : null
    } catch { return null }
    return null
  })
  const [remember, setRemember] = useState<boolean>(() => localStorage.getItem('rememberUser') === '1')
  const [loginError, setLoginError] = useState<string | null>(null)
  const [signupError, setSignupError] = useState<string | null>(null)
  const [mobileSearchOpen, setMobileSearchOpen] = useState(false)
  const searchInputRef = useRef<HTMLInputElement | null>(null)
  const [detailData, setDetailData] = useState<MovieDetail | ShowDetail | null>(null)
  const [detailLoading, setDetailLoading] = useState(false)
  const [detailError, setDetailError] = useState<string | null>(null)
  const primaryNav = [
    { id: 'analytics', label: 'Analytics' },
    { id: 'movies', label: 'Movies' },
    { id: 'tv', label: 'TV' },
  ]

  // Sync URL to state on mount and location change
  useEffect(() => {
    const path = location.pathname
    if (path === '/' || path === '/home') {
      setView('app')
      setTab('home')
    } else if (path === '/analytics') {
      setView('app')
      setTab('analytics')
    } else if (path === '/movies') {
      setView('app')
      setTab('movies')
    } else if (path === '/tv') {
      setView('app')
      setTab('tv')
    } else if (path === '/search') {
      setView('app')
      setTab('search')
    } else if (path === '/login') {
      setView('login')
    } else if (path === '/signup') {
      setView('signup')
    } else if (path === '/accounts') {
      setView('accounts')
    } else if (path.startsWith('/movie/') || path.startsWith('/show/')) {
      setView('detail')
    }
  }, [location.pathname])

  // Helper to navigate with state sync
  const navigateToTab = (newTab: string) => {
    const targetPath = `/${newTab === 'home' ? '' : newTab}`
    setTab(newTab)
    setView('app')
    navigate(targetPath)
  }

  const navigateToView = (newView: string) => {
    setView(newView)
    if(newView === 'app') {
      navigate('/')
    } else {
      navigate(`/${newView}`)
    }
  }

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

  useEffect(() => {
    if(!mobileSearchOpen) return
    searchInputRef.current?.focus()
  }, [mobileSearchOpen])

  useEffect(() => {
    if(typeof window === 'undefined') return
    const mq = window.matchMedia('(min-width: 640px)')
    const handler = () => setMobileSearchOpen(false)
    handler()
    if(mq.addEventListener){
      mq.addEventListener('change', handler)
    } else {
      mq.addListener(handler)
    }
    return () => {
      if(mq.removeEventListener){
        mq.removeEventListener('change', handler)
      } else {
        mq.removeListener(handler)
      }
    }
  }, [])

  useEffect(() => {
    if(view !== 'app'){
      setMobileSearchOpen(false)
    }
  }, [view])

  useEffect(() => {
    load()
  }, [])

  useEffect(() => {
    loadNewReleases()
  }, [newReleaseFilter])

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
        const results = await getTrending(trendingPeriod, 10)
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
  const newReleaseTotalPages = useMemo(() => {
    if(newReleases.length === 0) return 0
    return Math.ceil(newReleases.length / NEW_RELEASE_PAGE_SIZE)
  }, [newReleases.length])

  const displayedNewReleases = useMemo(() => {
    if(newReleases.length === 0) return []
    const clampedPage = Math.min(newReleasePage, Math.max(0, newReleaseTotalPages - 1))
    const start = clampedPage * NEW_RELEASE_PAGE_SIZE
    const end = start + NEW_RELEASE_PAGE_SIZE
    return newReleases.slice(start, end)
  }, [newReleases, newReleasePage, newReleaseTotalPages])


  useEffect(() => {
    if(newReleases.length === 0) {
      if(newReleasePage !== 0){
        setNewReleasePage(0)
      }
      return
    }
    if(newReleasePage > Math.max(0, newReleaseTotalPages - 1)){
      setNewReleasePage(Math.max(0, newReleaseTotalPages - 1))
    }
  }, [newReleases.length, newReleasePage, newReleaseTotalPages])

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

  async function loadMovies(page = 1){
    const targetPage = Math.max(1, page)
    setMoviesLoading(true)
    try{
      const data = await getList('movie', 'popularity', targetPage, LIST_PAGE_SIZE)
      const results = data?.results ?? []
      const total = typeof data?.total === 'number' ? data.total : results.length
      const resolvedPage = typeof data?.page === 'number' ? data.page : targetPage
      setMovies(results)
      setMoviesTotal(total)
      setMoviesPage(Math.max(1, resolvedPage))
    } catch (err) {
      console.error('Failed to load movies', err)
      setMovies([])
      setMoviesTotal(0)
      setMoviesPage(Math.max(1, targetPage))
    } finally {
      setMoviesLoading(false)
    }
  }

  async function loadTv(page = 1){
    const targetPage = Math.max(1, page)
    setTvLoading(true)
    try{
      const data = await getList('tv', 'popularity', targetPage, LIST_PAGE_SIZE)
      const results = data?.results ?? []
      const total = typeof data?.total === 'number' ? data.total : results.length
      const resolvedPage = typeof data?.page === 'number' ? data.page : targetPage
      setTv(results)
      setTvTotal(total)
      setTvPage(Math.max(1, resolvedPage))
    } catch (err) {
      console.error('Failed to load tv shows', err)
      setTv([])
      setTvTotal(0)
      setTvPage(Math.max(1, targetPage))
    } finally {
      setTvLoading(false)
    }
  }

  async function load(){
    setBusy(true)
    try{
      const summary = await getSummary().catch(()=>null)
      setSummary(summary)
      await Promise.all([
        loadMovies(1),
        loadTv(1),
      ])
    } finally {
      setBusy(false)
    }
  }

  async function loadNewReleases(limit = NEW_RELEASE_FETCH_LIMIT, filter: ReleaseFilter = newReleaseFilter){
    setNewReleasesLoading(true)
    setNewReleasesError(null)
    try {
      const items = await getNewReleases(limit, filter)
      const normalized = items.map(item => {
        const base: any = { ...item }
        if(typeof base.id !== 'number' && typeof base.item_id === 'number'){
          base.id = base.item_id
        }
        if(!base.poster_path && base.poster_url){
          base.poster_path = base.poster_url
        }
        if(base.media_type === 'show'){
          base.media_type = 'tv'
        }
        return base as MediaItem
      })
      const sorted = normalized
        .slice()
        .sort((a, b) => {
          const aTime = a.release_date ? Date.parse(a.release_date) : 0
          const bTime = b.release_date ? Date.parse(b.release_date) : 0
          return bTime - aTime
        })
      setNewReleases(sorted)
      setNewReleasePage(0)
    } catch (err) {
      console.error('Failed to load new releases', err)
      setNewReleases([])
      setNewReleasesError('Unable to load new releases at the moment.')
    } finally {
      setNewReleasesLoading(false)
    }
  }

  async function onRefresh(){
    setBusy(true)
    try{
      await refresh(1)
      await Promise.all([load(), loadNewReleases(undefined, newReleaseFilter)])
    } finally {
      setBusy(false)
    }
  }

  async function onSearch(ev: React.FormEvent){
    ev.preventDefault()
    if(!q.trim()) return
    setBusy(true)
    try{ const d = await search(q.trim()); setResults(d.results) } finally { setBusy(false) }
    navigateToTab('search')
    setMobileSearchOpen(false)
  }

  async function navigateToDetail(mediaType: 'movie' | 'tv', id?: number){
    const targetId = typeof id === 'number' ? id : Number(id)
    setDetailError(null)
    setDetailData(null)
    if(!Number.isFinite(targetId) || targetId <= 0){
      setDetailLoading(false)
      setView('detail')
      navigate(`/${mediaType}/0`)
      setDetailError('Details unavailable for this title.')
      return
    }
    const safeId = targetId as number
    setDetailLoading(true)
    setView('detail')
    navigate(`/${mediaType}/${safeId}`)
    try {
      if(mediaType === 'movie'){
        const data = await getMovieDetail(safeId)
        setDetailData(data)
      } else {
        const data = await getShowDetail(safeId)
        setDetailData(data)
      }
    } catch (err) {
      console.error('Failed to load detail', err)
      setDetailError('Failed to load details. Please try again.')
    } finally {
      setDetailLoading(false)
    }
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
          try {
            sessionStorage.setItem('currentUser', JSON.stringify(profile))
          } catch {}
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
          navigateToTab('home')
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
                onClick={()=> navigateToView('accounts')}
              >
                View Stored Accounts
              </button>
            </form>
            
             <div className="auth-card-footer">
               <p>Don't have an account? <button className="link-button" onClick={()=>navigateToView('signup')}>Sign up</button></p>
               <button className="back-link-button" onClick={()=>navigateToView('app')}>← Back to app</button>
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
                onClick={()=> navigateToView('accounts')}
              >
                View Stored Accounts
              </button>
            </form>
            
             <div className="auth-card-footer">
               <p>Already have an account? <button className="link-button" onClick={()=> { 
                 setCurrentUser(null);
                try { sessionStorage.removeItem('currentUser'); localStorage.removeItem('currentUser'); localStorage.removeItem('rememberUser') } catch {}
                 navigateToView('login');
               }}>Sign in</button></p>
               <button className="back-link-button" onClick={()=>navigateToView('app')}>← Back to app</button>
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
          <button className="back-link-button-top" onClick={()=>navigateToView('app')}>← Back to App</button>
          
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
            <button className="btn-primary btn-landing" onClick={()=>navigateToView('login')}>
              <span>Sign In</span>
              <span className="btn-arrow">→</span>
            </button>
            <button className="btn-secondary btn-landing" onClick={()=>navigateToView('signup')}>
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
          <button className="btn-link" onClick={()=>navigateToView('login')}>Back</button>
        </div>
        <p style={{fontSize:'12px', color:'#666', marginTop:'8px'}}>Passwords are stored hashed for security and shown here as stored values.</p>
      </div>
    )
  }

  if(view === 'detail'){
    const adult = detailData && isAdultContent({
      media_type: detailData.media_type,
      genres: detailData.genres,
      title: detailData.title
    })
    let chipLabel = 'TITLE'
    if(detailData?.media_type){
      chipLabel = detailData.media_type.toUpperCase()
    } else if(detailData && 'movie_id' in detailData){
      chipLabel = 'MOVIE'
    } else if(detailData && 'show_id' in detailData){
      chipLabel = 'TV'
  }
    const rawLanguage = typeof detailData?.original_language === 'string' ? detailData.original_language.trim() : ''
    const hasLanguage = rawLanguage.length > 0
    const languageCode = hasLanguage ? rawLanguage.toUpperCase() : null
    const friendlyLanguage = hasLanguage ? formatLanguageLabel(rawLanguage) : null
    const languageTitle = friendlyLanguage
      ? languageCode && friendlyLanguage.toUpperCase() !== languageCode
        ? `${friendlyLanguage} (${languageCode})`
        : friendlyLanguage
      : hasLanguage
        ? languageCode
        : 'Language unavailable'

  return (
      <div className="detail-container">
        {/* Header */}
        <div className="detail-header-shell">
          <header className={`header detail-page-header ${mobileSearchOpen ? 'header-mobile-search' : ''}`}>
            <div className="header-left">
              <div className="brand-group">
                <button type="button" className="brand-link" onClick={()=>{ navigateToTab('home'); }}>
                  Movie &amp; TV Analytics
                </button>
              </div>
              <form className={`header-search ${mobileSearchOpen ? 'show' : ''}`} onSubmit={onSearch}>
                <span className="search-icon" aria-hidden="true">🔍</span>
                <input
                  ref={searchInputRef}
                  placeholder="Search TMDb…"
                  value={q}
                  onChange={e=>setQ(e.target.value)}
                  autoComplete="off"
                />
                <button type="submit" className="search-submit">Search</button>
              </form>
              <button
                type="button"
                className="search-toggle"
                aria-label="Toggle search"
                aria-expanded={mobileSearchOpen}
                onClick={()=>setMobileSearchOpen(prev => !prev)}
              >
                <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
                  <circle cx="11" cy="11" r="6" />
                  <line x1="15.5" y1="15.5" x2="21" y2="21" />
                </svg>
              </button>
            </div>
            <div className="header-right">
              <nav className="header-nav" aria-label="Primary navigation">
                {primaryNav.map(item => (
                  <button
                    key={item.id}
                    type="button"
                    onClick={()=>{ navigateToTab(item.id); }}
                    aria-current={tab === item.id ? 'page' : undefined}
                    className={`nav-pill ${tab === item.id ? 'active' : ''}`}
                  >
                    {item.label}
                  </button>
                ))}
              </nav>
              <div className="header-actions">
                {currentUser && <span className="chip user-chip-blue header-user">User: {currentUser.user}</span>}
                {!currentUser ? (
                  <button type="button" className="btn-outline header-auth" onClick={()=>navigateToView('login')}>Log In</button>
                ) : (
                  <button
                    type="button"
                    className="btn-outline header-auth"
                    onClick={()=>{ 
                      setCurrentUser(null);
                      try { sessionStorage.removeItem('currentUser'); localStorage.removeItem('currentUser'); localStorage.removeItem('rememberUser') } catch {}
                    }}
                  >
                    Log Out
                  </button>
                )}
              </div>
            </div>
          </header>
        </div>

        {detailLoading && (
          <div className="detail-loading">
            <div className="loading-spinner"></div>
            <p>Loading details...</p>
          </div>
        )}

        {detailError && (
          <div className="detail-error">
            <span className="error-icon">⚠️</span>
            <p>{detailError}</p>
          </div>
        )}

        {detailData && (
          <div className="detail-view">
            {/* Backdrop Hero Section */}
            <div className="detail-backdrop" style={{
              backgroundImage: detailData.backdrop_path 
                ? `url(https://image.tmdb.org/t/p/original${detailData.backdrop_path})`
                : 'linear-gradient(135deg, #1a1a1f 0%, #0d0d10 100%)'
            }}>
              <div className="detail-backdrop-overlay"></div>
            </div>

            {/* Main Content */}
            <div className="detail-content">
              <div className="detail-main">
                {/* Poster Card */}
                <div className="detail-poster-card">
                  <div className="detail-poster-wrapper">
                    {detailData.poster_path ? (
                      <img 
                        src={`https://image.tmdb.org/t/p/w500${detailData.poster_path}`} 
                        alt={detailData.title}
                        className={`detail-poster-img${adult ? ' blurred' : ''}`}
                      />
                    ) : (
                      <div className="detail-poster-empty">
                        <span className="poster-icon">🎬</span>
                        <span>No Poster</span>
                      </div>
                    )}
                    {adult && <div className="detail-adult-overlay-badge">18+</div>}
                  </div>
                  
                  {/* Quick Stats on Poster */}
                  <div className="detail-poster-stats">
                    <div className="poster-stat">
                      <div className="poster-stat-icon">⭐</div>
                      <div className="poster-stat-content">
                        <div className="poster-stat-value">{detailData.vote_average?.toFixed(1) ?? 'N/A'}</div>
                        <div className="poster-stat-label">TMDb Score</div>
                      </div>
                    </div>
                    {detailData.vote_count && (
                      <div className="poster-stat-sub">{detailData.vote_count.toLocaleString()} votes</div>
                    )}
                  </div>
                </div>

                {/* Info Panel */}
                <div className="detail-info-panel">
                  <div className="detail-heading">
                    <div className="detail-type-badge">{chipLabel}</div>
                    <h1 className="detail-main-title">{detailData.title}</h1>
                  </div>

                  {/* Meta Info Row */}
                  <div className="detail-meta-row">
                    {detailData.media_type === 'movie' && (detailData as MovieDetail).release_year && (
                      <div className="meta-item">
                        <span className="meta-icon">📅</span>
                        <span>{(detailData as MovieDetail).release_year}</span>
                      </div>
                    )}
                    {detailData.media_type === 'tv' && (detailData as ShowDetail).first_air_date && (
                      <div className="meta-item">
                        <span className="meta-icon">📅</span>
                        <span>{(detailData as ShowDetail).first_air_date?.substring(0, 4) ?? 'Unknown'}</span>
                      </div>
                    )}
                    {detailData.media_type === 'movie' && (detailData as MovieDetail).runtime_minutes && (
                      <div className="meta-item">
                        <span className="meta-icon">⏱️</span>
                        <span>{(detailData as MovieDetail).runtime_minutes} min</span>
                      </div>
                    )}
                    {detailData.media_type === 'tv' && (detailData as ShowDetail).season_count !== undefined && (
                      <div className="meta-item">
                        <span className="meta-icon">📺</span>
                        <span>{(detailData as ShowDetail).season_count} season{(detailData as ShowDetail).season_count !== 1 ? 's' : ''}</span>
                      </div>
                    )}
                  </div>

                  {/* Genres */}
                  <div className="detail-genre-list">
                    <span className="detail-genre-tag detail-language-tag" title={`Original language: ${languageTitle}`}>
                      <span className="language-icon" aria-hidden="true">🌐</span>
                      <span>{languageTitle}</span>
                    </span>
                    {detailData.genres.map(g => (
                      <span key={g} className="detail-genre-tag">{g}</span>
                    ))}
                  </div>

                  {/* Overview */}
                  <div className="detail-overview-section">
                    <h3 className="section-title">Synopsis</h3>
                    {!adult && detailData.overview ? (
                      <p className="detail-synopsis">{detailData.overview}</p>
                    ) : adult ? (
                      <div className="detail-adult-notice">
                        <span className="adult-icon">🔞</span>
                        <span>Synopsis hidden for adult content</span>
                      </div>
                    ) : (
                      <p className="detail-synopsis no-data">No synopsis available.</p>
                    )}
                  </div>

                  {/* Stats Grid */}
                  <div className="detail-stats-section">
                    <h3 className="section-title">Statistics</h3>
                    <div className="detail-stats-grid">
                      {detailData.user_avg_rating && (
                        <div className="stat-card">
                          <div className="stat-icon">👥</div>
                          <div className="stat-value">{detailData.user_avg_rating.toFixed(1)}</div>
                          <div className="stat-label">User Rating</div>
                        </div>
                      )}
                      <div className="stat-card">
                        <div className="stat-icon">💬</div>
                        <div className="stat-value">{detailData.review_count}</div>
                        <div className="stat-label">Reviews</div>
                      </div>
                      {detailData.popularity && (
                        <div className="stat-card">
                          <div className="stat-icon">🔥</div>
                          <div className="stat-value">{detailData.popularity.toFixed(0)}</div>
                          <div className="stat-label">Popularity</div>
                        </div>
                      )}
                      {detailData.media_type === 'movie' && (detailData as MovieDetail).budget && (detailData as MovieDetail).budget! > 0 && (
                        <div className="stat-card">
                          <div className="stat-icon">💰</div>
                          <div className="stat-value">${((detailData as MovieDetail).budget! / 1000000).toFixed(1)}M</div>
                          <div className="stat-label">Budget</div>
                        </div>
                      )}
                      {detailData.media_type === 'movie' && (detailData as MovieDetail).revenue && (detailData as MovieDetail).revenue! > 0 && (
                        <div className="stat-card">
                          <div className="stat-icon">📈</div>
                          <div className="stat-value">${((detailData as MovieDetail).revenue! / 1000000).toFixed(1)}M</div>
                          <div className="stat-label">Box Office</div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}
      </div>
    )
  }

  return (
    <div className="container">
      <header className={`header ${mobileSearchOpen ? 'header-mobile-search' : ''}`}>
        <div className="header-left">
          <div className="brand-group">
            <button type="button" className="brand-link" onClick={()=>{ navigateToTab('home'); }}>
              Movie &amp; TV Analytics
            </button>
          </div>
          <form className={`header-search ${mobileSearchOpen ? 'show' : ''}`} onSubmit={onSearch}>
            <span className="search-icon" aria-hidden="true">🔍</span>
            <input
              ref={searchInputRef}
              placeholder="Search TMDb…"
              value={q}
              onChange={e=>setQ(e.target.value)}
              autoComplete="off"
            />
            <button type="submit" className="search-submit">Search</button>
        </form>
          <button
            type="button"
            className="search-toggle"
            aria-label="Toggle search"
            aria-expanded={mobileSearchOpen}
            onClick={()=>setMobileSearchOpen(prev => !prev)}
          >
            <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.8">
              <circle cx="11" cy="11" r="6" />
              <line x1="15.5" y1="15.5" x2="21" y2="21" />
            </svg>
          </button>
        </div>
        <div className="header-right">
          <nav className="header-nav" aria-label="Primary navigation">
            {primaryNav.map(item => (
              <button
                key={item.id}
                type="button"
                onClick={()=>navigateToTab(item.id)}
                aria-current={tab === item.id ? 'page' : undefined}
                className={`nav-pill ${tab === item.id ? 'active' : ''}`}
              >
                {item.label}
              </button>
            ))}
          </nav>
          <div className="header-actions">
            {currentUser && <span className="chip user-chip-blue header-user">User: {currentUser.user}</span>}
            {!currentUser ? (
              <button type="button" className="btn-outline header-auth" onClick={()=>setView('login')}>Log In</button>
            ) : (
              <button
                type="button"
                className="btn-outline header-auth"
                onClick={()=>{ 
                  setCurrentUser(null);
                  try { sessionStorage.removeItem('currentUser'); localStorage.removeItem('currentUser'); localStorage.removeItem('rememberUser') } catch {}
                }}
              >
                Log Out
              </button>
            )}
          </div>
        </div>
      </header>
      {tab==='home' && (
        <section className="hero">
          <h1>Movie &amp; TV Analytics</h1>
          <p>Track trends, analyze ratings, and discover insights across your favorite movies and shows.</p>
          <div className="hero-actions">
            <button className="btn-solid btn-lg" onClick={()=>navigateToTab('analytics')}>Get Started</button>
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
                  const adult = isAdultContent(item)
                  return (
                    <div
                      key={`hero-${item.media_type}-${item.tmdb_id}-${idx}`}
                      className={`trending-hero-slide${isActive ? ' active' : ''}${adult ? ' adult-slide' : ''}`}
                    >
                      <div
                        className="trending-hero-backdrop"
                        style={backdrop ? { backgroundImage: `url(${backdrop})` } : undefined}
                      />
                      <div className="trending-hero-overlay" />
                      <div className="trending-hero-content">
                        {adult && <div className="adult-content-banner">Adult content hidden</div>}
                        <div className="trending-hero-pill">{item.media_type === 'movie' ? 'Movie' : 'TV Series'}</div>
                        <h2>{item.title}</h2>
                        <p>{adult ? 'Adult synopsis hidden.' : overview || 'No synopsis available just yet.'}</p>
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
                    <span className="trending-hero-nav-icon">‹</span>
                  </button>
                  <button
                    type="button"
                    className="trending-hero-nav next"
                    onClick={() => setCarouselIndex(prev => (prev + 1) % heroSlides.length)}
                    aria-label="Next slide"
                  >
                    <span className="trending-hero-nav-icon">›</span>
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
                  <h3>Popular</h3>
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

              <div className="trending-rail-list" key={`trending-${trendingPeriod}`}>
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
                  const adult = isAdultContent(item)
                  const targetMediaType = item.media_type === 'movie' ? 'movie' : 'tv'
                  const handleClick = () => {
                    navigateToDetail(targetMediaType, item.item_id || undefined)
                  }
                  return (
                    <button
                      key={`leaderboard-${item.media_type}-${item.tmdb_id}-${index}`}
                      type="button"
                      className={`trending-rail-item${adult ? ' trending-rail-item-adult' : ''}`}
                      onClick={handleClick}
                      aria-label={`View details for ${item.title}`}
                    >
                      <div className="rank">{index + 1}</div>
                      <div className={`thumb${adult ? ' thumb-adult' : ''}`}>
                        {poster ? <img src={poster} alt={item.title} loading="lazy" /> : <div className="thumb-placeholder">🎬</div>}
                        {adult && <div className="thumb-adult-overlay">18+</div>}
                      </div>
                      <div className="details">
                        <div className="title" title={item.title}>{item.title}</div>
                        <div className="meta">
                          <span>{item.media_type === 'movie' ? 'Movie' : 'TV'}</span>
                          <span>⭐ {item.tmdb_vote_avg != null ? item.tmdb_vote_avg.toFixed(1) : '—'}</span>
                        </div>
                        <div className="genres">{adult ? 'Restricted content' : genres}</div>
                      </div>
                    </button>
                  )
                })}
              </div>
            </aside>
          </div>

          <div className="new-release-section">
            <div className="new-release-header">
              <div>
                <h3>New Releases</h3>
                <p>Fresh arrivals from {new Date().getFullYear()}.</p>
              </div>
              <div className="new-release-actions">
                <div className="new-release-tabs">
                  {(['all', 'movie', 'tv'] as ReleaseFilter[]).map(option => (
                    <button
                      key={option}
                      type="button"
                      className={option === newReleaseFilter ? 'active' : ''}
                      onClick={() => { setNewReleaseFilter(option); setNewReleasePage(0) }}
                      disabled={newReleasesLoading && option === newReleaseFilter}
                    >
                      {option === 'all' ? 'All' : option === 'movie' ? 'Movies' : 'TV Shows'}
                    </button>
                  ))}
                </div>
                <button
                  type="button"
                  className="btn-text"
                  onClick={() => loadNewReleases(NEW_RELEASE_FETCH_LIMIT, newReleaseFilter)}
                  disabled={newReleasesLoading}
                >
                  {newReleasesLoading ? 'Refreshing…' : 'Refresh list'}
                </button>
              </div>
            </div>

            {newReleasesLoading && newReleases.length === 0 && (
              <div className="new-release-empty">Loading new releases…</div>
            )}
            {!newReleasesLoading && newReleasesError && (
              <div className="new-release-empty">{newReleasesError}</div>
            )}
            {!newReleasesLoading &&
              !newReleasesError &&
              newReleases.length === 0 && (
                <div className="new-release-empty">
                  No new releases found. Try running the ETL loader.
                </div>
              )}
            {newReleases.length > 0 && (
              <>
                <div className="new-release-grid-wrapper">
                  <div className="new-release-grid" key={`new-release-${newReleaseFilter}-${newReleasePage}`}>
                    {displayedNewReleases.map(item => (
                      <Card 
                        key={`nr-${item.media_type}-${item.tmdb_id}`} 
                        item={item} 
                        onClick={() => navigateToDetail(item.media_type, item.id!)}
                      />
                    ))}
                  </div>
                  {newReleasesLoading && (
                    <div className="list-overlay">Refreshing…</div>
                  )}
                </div>
                {newReleaseTotalPages > 1 && (
                  <div className="pagination-controls" aria-label="New releases pagination">
                    <button
                      type="button"
                      onClick={() => setNewReleasePage(prev => Math.max(0, prev - 1))}
                      disabled={newReleasesLoading || newReleasePage === 0}
                    >
                      ‹ Previous
                    </button>
                    <button
                      type="button"
                      onClick={() => setNewReleasePage(prev => Math.min(newReleaseTotalPages - 1, prev + 1))}
                      disabled={newReleasesLoading || newReleasePage >= newReleaseTotalPages - 1}
                    >
                      Next ›
                    </button>
                  </div>
                )}
              </>
            )}
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
          <div className="grid">
            {movies.map(m => (
              <Card 
                key={`m-${m.tmdb_id}`} 
                item={m} 
                onClick={() => navigateToDetail('movie', m.id!)}
              />
            ))}
          </div>
          {moviesTotal > LIST_PAGE_SIZE && (
            <div className="pagination-controls" aria-label="Movies pagination">
              <button
                type="button"
                onClick={() => loadMovies(moviesPage - 1)}
                disabled={moviesLoading || moviesPage <= 1}
              >
                ‹ Previous
              </button>
              <button
                type="button"
                onClick={() => loadMovies(moviesPage + 1)}
                disabled={
                  moviesLoading ||
                  moviesTotal === 0 ||
                  moviesPage * LIST_PAGE_SIZE >= moviesTotal
                }
              >
                Next ›
              </button>
            </div>
          )}
        </section>
      )}

      {tab==='tv' && (
        <section>
          <h3>Top TV</h3>
          <div className="grid">
            {tv.map(m => (
              <Card 
                key={`t-${m.tmdb_id}`} 
                item={m} 
                onClick={() => navigateToDetail('tv', m.id!)}
              />
            ))}
          </div>
          {tvTotal > LIST_PAGE_SIZE && (
            <div className="pagination-controls" aria-label="TV pagination">
              <button
                type="button"
                onClick={() => loadTv(tvPage - 1)}
                disabled={tvLoading || tvPage <= 1}
              >
                ‹ Previous
              </button>
              <button
                type="button"
                onClick={() => loadTv(tvPage + 1)}
                disabled={
                  tvLoading ||
                  tvTotal === 0 ||
                  tvPage * LIST_PAGE_SIZE >= tvTotal
                }
              >
                Next ›
              </button>
            </div>
          )}
        </section>
      )}

      {tab==='search' && (
        <section>
          <h3>Search Results{q ? ` for "${q}"` : ''}</h3>
          {results.length === 0 && !busy ? (
            <div style={{textAlign:'center', padding:'48px 24px', color:'var(--muted)'}}>
              No results found. Try a different search term.
            </div>
          ) : (
            <div className="grid">
              {results.map(m => (
                <Card 
                  key={`s-${m.media_type}-${m.tmdb_id}`} 
                  item={m} 
                  onClick={() => navigateToDetail(m.media_type, m.id!)}
                />
              ))}
            </div>
          )}
        </section>
      )}

      {busy && <div className="busy">Loading…</div>}
    </div>
  )
}
