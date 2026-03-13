import { useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { 
  ArrowLeft, 
  Upload, 
  FileSpreadsheet, 
  Package, 
  CheckCircle,
  CheckCircle2, 
  AlertCircle,
  Download,
  Eye,
  Loader2,
  Layers,
  Zap,
  Settings,
  FileText,
  RefreshCw,
  Search,
  Users,
  GitBranch
} from 'lucide-react';
import './App.css';
import ProductFamilyManager from './components/ProductFamilyManager';

// API endpoint - auto-detect based on environment
const getApiUrl = () => {
  if (import.meta.env.VITE_API_URL) {
    return import.meta.env.VITE_API_URL;
  }
  const isLocalhost = window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1';
  if (isLocalhost && window.location.port !== '8000') {
    return 'http://localhost:8000';
  }
  return '';
};
const API_URL = getApiUrl();

// Helper to convert relative image URLs to absolute
const getImageUrl = (url) => {
  if (!url) return '';
  // Already absolute URL
  if (url.startsWith('http://') || url.startsWith('https://')) {
    return url;
  }
  // Relative URL - prepend API base
  const baseUrl = API_URL || window.location.origin;
  return `${baseUrl}${url.startsWith('/') ? '' : '/'}${url}`;
};

// Helper to clean product name from UUID prefix
const cleanProductName = (name) => {
  if (!name) return '';
  // UUID pattern at start: 8-4-4-4-12 hex chars
  const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\s*/i;
  return name.replace(uuidPattern, '').trim();
};

// Helper to ensure title starts with brand
const ensureBrandInTitle = (title, brand = 'KADAX') => {
  if (!title) return '';
  const cleanTitle = cleanProductName(title);
  if (!cleanTitle) return '';
  // Check if already starts with brand
  if (cleanTitle.toUpperCase().startsWith(brand.toUpperCase())) {
    return cleanTitle;
  }
  return `${brand} ${cleanTitle}`;
};

// Supported markets
const MARKETS = [
  { code: 'DE', name: 'Germany', flag: '🇩🇪', currency: 'EUR' },
  { code: 'FR', name: 'France', flag: '🇫🇷', currency: 'EUR' },
  { code: 'IT', name: 'Italy', flag: '🇮🇹', currency: 'EUR' },
  { code: 'ES', name: 'Spain', flag: '🇪🇸', currency: 'EUR' },
  { code: 'NL', name: 'Netherlands', flag: '🇳🇱', currency: 'EUR' },
  { code: 'PL', name: 'Poland', flag: '🇵🇱', currency: 'PLN' },
  { code: 'SE', name: 'Sweden', flag: '🇸🇪', currency: 'SEK' },
  { code: 'BE', name: 'Belgium', flag: '🇧🇪', currency: 'EUR' },
];

const COLUMN_LABELS = {
  KNumber: 'K-Number',
  SKU: 'SKU',
  Title: 'Name',
  ProductType: 'Product Type',
  Status: 'Status',
};

// Complete Amazon Product Types list (from Category Listing Templates)
const AMAZON_PRODUCT_TYPES = [
  // Garden & Outdoor - KADAX main categories
  "PLANTER", "FLOWER_POT", "PLANT_CONTAINER", "PLANT_POT", 
  "GREENHOUSE", "CLIMBING_PLANT_SUPPORT_STRUCTURE", "GARDEN_EDGING", "FENCE_PANEL",
  "IRRIGATION_SPRINKLER", "DRIP_IRRIGATION_KIT", "WATERING_CAN", "HOSE_PIPE_FITTING",
  "COMPOST_BIN", "SANDBOX", "ANCHOR_STAKE", "FERTILIZER",
  "RAKE", "SHOVEL_SPADE", "HOE", "AXE", "GARDEN_SHEAR_SCISSORS", "GARDEN_TOOL_SET",
  "ABIS_LAWN_AND_GARDEN", "OUTDOOR_LIVING",
  // Kitchen
  "KITCHEN", "COOKING_POT", "SAUTE_FRY_PAN", "ROASTING_PAN", "DUTCH_OVENS", "CASSEROLES",
  "BAKING_PAN", "BAKING_MAT", "SHEET_PAN", "CONTAINER_LID",
  "FOOD_STORAGE_CONTAINER", "JAR", "BOTTLE", "PITCHER",
  "CUTTING_BOARD", "KITCHEN_KNIFE", "KNIFE_SHARPENER", "SCISSORS",
  "FOOD_GRATER", "FOOD_SLICER", "BLADED_FOOD_PEELER", "CORE_PIT_REMOVER",
  "FOOD_SPATULA", "TONG_UTENSIL", "WHISK_UTENSIL", "SPOON", "FORK", "FLATWARE",
  "MEASURING_CUP", "FOOD_SCOOP", "FUNNEL", "SKEWER", "ROLLING_PIN",
  "COOKIE_CUTTER", "FOOD_PREPARATION_MOLD", "PASTRY_BASTING_BRUSH",
  "MORTAR_AND_PESTLE_SET", "GARLIC_PRESS", "MANUAL_FOOD_MILL_GRINDER",
  "JUICER", "PASTA_MAKER", "CAN_OPENER", "BOTTLE_OPENER", "BOTTLE_STOPPER",
  "COFFEE_FILTER", "COFFEE_MAKER", "TEAPOT", "STOVETOP_KETTLE",
  "DRAIN_STRAINER", "FOOD_STRAINER", "TRIVET",
  "ICE_CUBE_TRAY", "GRAVITY_BEVERAGE_DISPENSER",
  "DISHWARE_BOWL", "DISHWARE_PLATE", "DISHWARE_PLACE_SETTING", "DRINKING_CUP", "DRINKING_STRAW",
  "BUTTER_DISH", "NAPKIN_HOLDER", "CUP_HOLDER", "BOTTLE_RACK",
  "DRYING_RACK", "DISH_DRYING_MAT", "FOOD_WRAP",
  // Home & Storage
  "HOME", "HOME_BED_AND_BATH", "HOME_FURNITURE_AND_DECOR", "HOME_ORGANIZERS_AND_STORAGE",
  "STORAGE_BOX", "STORAGE_BAG", "STORAGE_DRAWER", "STORAGE_RACK", "STORAGE_HOOK", "STORAGE_COVER",
  "BASKET", "BUCKET", "TRAY", "CADDY", "TRASH_CAN",
  "CLOTHES_HANGER", "CLOTHES_PIN", "CLOTHES_RACK", "LAUNDRY_HAMPER",
  "SHELF", "CABINET", "MOUNTED_STORAGE_SYSTEM_KIT",
  "IRONING_BOARD", "IRONING_BOARD_COVER", "TEXTILE_IRON",
  // Furniture
  "FURNITURE", "FURNITURE_COVER", "FURNITURE_FLOOR_PROTECTOR", "FURNITURE_LEG",
  "TABLE", "CHAIR", "BENCH", "STOOL_SEATING", "OTTOMAN", "BEAN_BAG_CHAIR",
  "LAP_DESK", "WORKBENCH", "STEP_STOOL", "LADDER",
  // Bathroom
  "BATHROOM_CONTAINER_SET", "BATHTUB_SHOWER_MAT", "SOAP_DISH", "PUMP_DISPENSER",
  "TOOTHBRUSH_HOLDER", "TOILET_PAPER_HOLDER", "TOILET_SEAT", "TOWEL_HOLDER",
  "BABY_BATHTUB",
  // Decor
  "VASE", "FIGURINE", "SCULPTURE", "WALL_ART", "DECORATIVE_SIGNAGE",
  "CANDLE", "CANDLE_HOLDER", "HANGING_ORNAMENT", "GARLAND", "WREATH",
  "WIND_CHIME", "WIND_SPINNER", "ARTIFICIAL_PLANT", "ARTIFICIAL_TREE",
  "TREE_SKIRT", "TREE_TOPPER", "BLOWER_INFLATED_DECORATION",
  "PILLOW", "DECORATIVE_PILLOW_COVER", "BLANKET", "TOWEL", "TABLE_RUNNER", "RUG", "RUG_PAD", "CARPETING",
  // Pet Supplies
  "PET_SUPPLIES", "PET_BED_MAT", "LITTER_BOX", "ANIMAL_CAGE", "ANIMAL_CARRIER", "ANIMAL_SHELTER", "WILDLIFE_FEEDER",
  // Cleaning
  "BROOM", "MOP", "MOP_BUCKET_SET", "SQUEEGEE", "UTILITY_SPONGE", "CLEANING_BRUSH", "CLEANING_AGENT",
  "SPRAY_BOTTLE", "PUMP_SPRAYER", "WASTE_BAG",
  // Tools & Hardware
  "TOOLS", "HARDWARE", "HARDWARE_TUBING", "DRILL_BITS", "SAW", "HAMMER_MALLET",
  "PAINT_BRUSH", "ADHESIVE_TAPES", "BUNGEE_CORD", "THREAD_CORD",
  "PROTECTIVE_GLOVE", "KNEE_PAD", "PORTABLE_TOOL_BOX",
  // Auto
  "AUTO_ACCESSORY", "VEHICLE_MAT", "VEHICLE_SEAT_COVER",
  // Other categories
  "LIGHT_FIXTURE", "UMBRELLA", "AWNING", "FREESTANDING_SHELTER", "TARP", "NETTING_COVER",
  "SWING", "RAMP", "SLED", "FIRE_PIT", "ICE_CHEST",
  "PICNIC_HOLDER", "MEAL_HOLDER", "UTILITY_CART_WAGON",
  "DOOR", "FAUCET", "VALVE", "SINK", "CISTERN", "ELECTRONIC_SWITCH",
  "ANTI_FATIGUE_FLOOR_MAT", "ASHTRAY", "KEYCHAIN", "THERMOMETER",
  "ENVELOPE", "LABEL", "MARKING_PEN", "OFFICE_PRODUCTS",
  "LUGGAGE", "CARRIER_BAG_CASE", "PACKING_MATERIAL", "SHIPPING_BOX",
  "SPORTING_GOODS", "KITE", "BICYCLE_BASKET", "BOARD_GAME", "TOYS_AND_GAMES", "TOY_FIGURE",
  "BABY_SEAT", "BODY_POSITIONER", "HEALTH_PERSONAL_CARE", "MAKEUP_VANITY", "JEWELRY_STORAGE",
  "BUILDING_MATERIAL", "RAW_MATERIALS", "LAB_SUPPLY", "CONSUMER_ELECTRONICS",
  "PANTS", "SHOES", "MEAT", "INSECT_REPELLENT", "PEST_CONTROL_DEVICE", "SOLID_FIRE_FUEL", "LIQUID_FUEL_CONTAINER", "SOIL_TILLER_CULTIVATOR"
].sort();

function App() {
  const [step, setStep] = useState(1); // 1: upload, 2: review, 3: configure, 4: content-review, 5: export
  const [searchType, setSearchType] = useState('k_number'); // 'k_number' or 'ean'
  const [kNumbers, setKNumbers] = useState('');
  const [parsedData, setParsedData] = useState(null);
  const [families, setFamilies] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [selectedMarkets, setSelectedMarkets] = useState(['DE']); // Multi-market selection
  const [exportFormat, setExportFormat] = useState('amazon');
  const [sessionId, setSessionId] = useState(null);
  const [aiContent, setAiContent] = useState(null); // AI-generated content
  const [generatingContent, setGeneratingContent] = useState(false);
  const [selectedProduct, setSelectedProduct] = useState(null); // For content preview
  const [aiPreview, setAiPreview] = useState(null); // AI generation preview
  const [aiPreviewLoading, setAiPreviewLoading] = useState(false);
  const [aiLogs, setAiLogs] = useState([]); // Real-time AI generation logs
  const [aiLogsVisible, setAiLogsVisible] = useState(false);
  
  // Content Review state (Step 4)
  const [editableContent, setEditableContent] = useState({}); // Per product, per market editable content
  const [activeProduct, setActiveProduct] = useState(null); // Currently selected product for review
  const [activeMarket, setActiveMarket] = useState(null); // Currently selected market tab
  const [marketStatus, setMarketStatus] = useState({}); // Track approval status per product/market
  const [editingField, setEditingField] = useState(null); // Currently editing field
  
  // Product Family Builder state
  const [showFamilyBuilder, setShowFamilyBuilder] = useState(false);
  const [selectedProductType, setSelectedProductType] = useState('CONTAINER_LID'); // Default product type for families
  
  // SKU Generator config
  const [skuConfig, setSkuConfig] = useState({
    source: 'sku',
    prefix: '',
    suffix: '',
    replace_from: '',
    replace_to: ''
  });
  const [skuPreview, setSkuPreview] = useState([]);
  
  // Attributes panel expanded state
  const [expandedAttributes, setExpandedAttributes] = useState(false);
  
  // A+ Content generation state
  const [aplusGenerating, setAplusGenerating] = useState(false);
  
  // Regenerate dropdown state
  const [showRegenerateMenu, setShowRegenerateMenu] = useState(false);
  const [regenerateFields, setRegenerateFields] = useState({
    title: true,
    bullets: true,
    description: true,
    keywords: true
  });
  const [regenerating, setRegenerating] = useState(false);

  // Add log entry for AI generation
  const addAiLog = (type, message, details = null) => {
    const timestamp = new Date().toLocaleTimeString('pl-PL');
    setAiLogs(prev => [...prev, { timestamp, type, message, details }]);
  };

  // Clear AI logs
  const clearAiLogs = () => setAiLogs([]);
  
  // Update editable content
  const updateContent = (kNumber, market, field, value) => {
    setEditableContent(prev => ({
      ...prev,
      [kNumber]: {
        ...prev[kNumber],
        [market]: {
          ...prev[kNumber]?.[market],
          [field]: value,
          modified: true
        }
      }
    }));
  };
  
  // Update bullet point
  const updateBullet = (kNumber, market, index, value) => {
    setEditableContent(prev => {
      const bullets = [...(prev[kNumber]?.[market]?.bullets || [])];
      bullets[index] = value;
      return {
        ...prev,
        [kNumber]: {
          ...prev[kNumber],
          [market]: {
            ...prev[kNumber]?.[market],
            bullets,
            modified: true
          }
        }
      };
    });
  };
  
  // Reorder images
  const moveImage = (kNumber, fromIndex, toIndex) => {
    setEditableContent(prev => {
      const images = [...(prev[kNumber]?.images || [])];
      const [moved] = images.splice(fromIndex, 1);
      images.splice(toIndex, 0, moved);
      return {
        ...prev,
        [kNumber]: {
          ...prev[kNumber],
          images,
          imagesModified: true
        }
      };
    });
  };
  
  // Set market approval status
  const setMarketApproval = (kNumber, market, status) => {
    setMarketStatus(prev => ({
      ...prev,
      [`${kNumber}_${market}`]: status // 'pending', 'approved', 'needs-edit', 'regenerate'
    }));
  };
  
  // Regenerate selected fields for a product/market
  const regenerateContent = async (kNumber, market, fields) => {
    if (!sessionId) {
      setError('Brak sesji - nie można regenerować');
      return;
    }
    
    const selectedFields = Object.entries(fields).filter(([_, v]) => v).map(([k]) => k);
    if (selectedFields.length === 0) {
      setError('Wybierz co najmniej jedno pole do regeneracji');
      return;
    }
    
    setRegenerating(true);
    setShowRegenerateMenu(false);
    addAiLog('info', `🔄 Regeneruję ${selectedFields.join(', ')} dla ${kNumber} / ${market}...`);
    
    try {
      const res = await fetch(`${API_URL}/api/productonboard/sessions/${sessionId}/regenerate-field`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          k_number: kNumber,
          market: market,
          fields: selectedFields
        })
      });
      
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || `HTTP ${res.status}`);
      }
      
      const data = await res.json();
      
      // Update editable content with regenerated fields
      if (data.content) {
        setEditableContent(prev => ({
          ...prev,
          [kNumber]: {
            ...prev[kNumber],
            [market]: {
              ...prev[kNumber]?.[market],
              ...data.content
            }
          }
        }));
        addAiLog('success', `✅ Zregenerowano: ${selectedFields.join(', ')}`);
      }
      
    } catch (err) {
      addAiLog('error', `❌ Błąd regeneracji: ${err.message}`);
      setError(`Błąd regeneracji: ${err.message}`);
    } finally {
      setRegenerating(false);
    }
  };
  
  // Check if all markets are approved
  const allMarketsApproved = () => {
    if (!editableContent || Object.keys(editableContent).length === 0) return false;
    if (!selectedMarkets.length) return false;
    
    for (const kNumber of Object.keys(editableContent)) {
      for (const market of selectedMarkets) {
        const status = marketStatus[`${kNumber}_${market}`];
        if (status !== 'approved') return false;
      }
    }
    return true;
  };

  // Toggle market selection
  const toggleMarket = (code) => {
    setSelectedMarkets(prev => 
      prev.includes(code) 
        ? prev.filter(m => m !== code)
        : [...prev, code]
    );
  };

  const processKNumbers = async () => {
    const identifiers = kNumbers.trim().split(/[\n,]+/).map(k => k.trim()).filter(k => k);
    if (identifiers.length === 0) {
      setError(searchType === 'k_number' ? 'Wpisz co najmniej jeden K-number' : 'Wpisz co najmniej jeden kod EAN');
      return;
    }
    
    if (selectedMarkets.length === 0) {
      setError('Wybierz co najmniej jeden rynek docelowy');
      return;
    }
    
    setLoading(true);
    setError('');
    
    try {
      // Build request based on search type
      const requestBody = {
        market: selectedMarkets[0], // Primary market
        target_markets: selectedMarkets,
        auto_analyze: true,
        generate_draft: false,
        generate_ai_content: false, // Will generate after review
        sku_config: skuConfig, // Pass SKU generation config
        search_type: searchType,
      };
      
      // Add identifiers based on search type
      if (searchType === 'ean') {
        requestBody.ean_codes = identifiers;
        requestBody.k_numbers = []; // Empty for EAN search
      } else {
        requestBody.k_numbers = identifiers;
        requestBody.ean_codes = [];
      }
      
      // Call backend API
      const res = await fetch(`${API_URL}/api/productonboard/quick-onboard`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });
      
      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || `HTTP ${res.status}`);
      }
      
      const data = await res.json();
      setSessionId(data.session_id);
      
      // Map API response to UI format
      // Backend returns: pim_data.products for PIM data, analyses for analysis results
      const pimProducts = data.pim_data?.products || [];
      const analyses = data.analysis?.results || data.analyses || [];
      
      const mockData = {
        totalRows: pimProducts.length,
        columns: ['KNumber', 'SKU', 'Title', 'ProductType', 'Status'],
        preview: pimProducts.map(p => {
          // Find matching analysis
          const analysis = analyses.find(a => a.k_number === p.k_number) || {};
          
          // Determine status based on validation
          let status = '❌ Not found';
          if (p.found) {
            if (analysis.ready_for_amazon) {
              status = '✅ Ready';
            } else if (analysis.validation_status === 'warnings') {
              status = '⚠️ Warnings';
            } else if (analysis.validation_errors?.length > 0) {
              status = '❌ Issues';
            } else {
              status = '✅ Ready';
            }
          }
          
          return {
            SKU: p.sku || '',
            KNumber: p.k_number,
            EAN: p.ean || p.attributes?.ean || p.attributes?.gtin || '',
            Title: p.name || p.attributes?.tytul_seo || p.attributes?.nazwa || 'Bez nazwy',
            ProductType: analysis.amazon_product_type || 'AUTO', // AI-detected!
            Status: status,
            // Validation
            Errors: analysis.validation_errors?.join(', ') || '',
            Warnings: analysis.validation_warnings?.join(', ') || '',
            ValidationDetails: analysis.validation_errors || [],
            WarningDetails: analysis.validation_warnings || [],
            // PIM data quality indicators
            HasDescription: analysis.has_description || !!p.description,
            HasImages: analysis.has_images || (p.images?.length > 0),
            ImageCount: analysis.image_count || p.images?.length || 0,
            // Raw data for AI preview
            rawData: p
          };
        })
      };
      
      setParsedData(mockData);
      setFamilies([]);
      setStep(2);
    } catch (err) {
      setError('Błąd pobierania z PIM: ' + err.message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app">
      <header className="header">
        <a href="/" className="back-btn">
          <ArrowLeft size={18} />
          Dashboard
        </a>
        
        <div className="header-title">
          <div className="header-icon">
            <Package size={24} />
          </div>
          <div>
            <h1>ProductOnboard</h1>
            <span className="subtitle">Product Import & Family Builder</span>
          </div>
        </div>
        
        <div className="header-badge">
          <Zap size={14} />
          AI-Powered
        </div>
      </header>

      <main className="main">
        {/* Progress Steps */}
        <div className="progress-steps">
          {[
            { num: 1, label: 'PIM', icon: Upload },
            { num: 2, label: 'Review', icon: Eye },
            { num: 3, label: 'Configure', icon: Settings },
            { num: 4, label: 'Content', icon: FileText },
            { num: 5, label: 'Export', icon: Download }
          ].map((s, i) => (
            <div key={s.num} className={`step ${step >= s.num ? 'active' : ''} ${step > s.num ? 'completed' : ''}`}>
              <div className="step-icon">
                {step > s.num ? <CheckCircle2 size={20} /> : <s.icon size={20} />}
              </div>
              <span>{s.label}</span>
              {i < 4 && <div className="step-line" />}
            </div>
          ))}
        </div>

        {error && (
          <div className="error-box">
            <AlertCircle size={18} />
            {error}
          </div>
        )}

        <AnimatePresence mode="wait">
          {/* Step 1: PIM */}
          {step === 1 && (
            <motion.div
              key="step1"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className="step-content"
            >
              <div className="knumbers-section">
                <div className="section-header">
                  <h3>Wyszukaj produkty w PIM</h3>
                  <p className="section-hint">Wyszukaj produkty po numerze K (Kxxxx) lub kodzie EAN.</p>
                </div>
                
                {/* Search Type Toggle */}
                <div className="search-type-toggle">
                  <button 
                    className={`toggle-btn ${searchType === 'k_number' ? 'active' : ''}`}
                    onClick={() => setSearchType('k_number')}
                  >
                    🏷️ K-Number
                  </button>
                  <button 
                    className={`toggle-btn ${searchType === 'ean' ? 'active' : ''}`}
                    onClick={() => setSearchType('ean')}
                  >
                    📦 EAN
                  </button>
                </div>
                
                <textarea
                  className="knumbers-input"
                  value={kNumbers}
                  onChange={(e) => setKNumbers(e.target.value)}
                  placeholder={searchType === 'k_number' 
                    ? "K798\nK1234\nK5678\n...lub rozdzielone przecinkami: K798, K1234"
                    : "5903699455531\n5903699455548\n...lub rozdzielone przecinkami"
                  }
                  rows={6}
                />
                
                <div className="knumbers-stats">
                  <span>{kNumbers.trim().split(/[\n,]+/).filter(k => k.trim()).length} {searchType === 'k_number' ? 'K-numbers' : 'EAN codes'}</span>
                </div>
                
                {/* Target Markets Selection */}
                <div className="markets-section">
                  <div className="section-header">
                    <h3>🌍 Rynki docelowe</h3>
                    <p className="section-hint">Wybierz rynki Amazon, na które chcesz przygotować produkty</p>
                  </div>
                  <div className="markets-grid">
                    {MARKETS.map(market => (
                      <button
                        key={market.code}
                        className={`market-btn ${selectedMarkets.includes(market.code) ? 'active' : ''}`}
                        onClick={() => toggleMarket(market.code)}
                      >
                        <span className="market-flag">{market.flag}</span>
                        <span className="market-code">{market.code}</span>
                      </button>
                    ))}
                  </div>
                  <div className="selected-markets-info">
                    Wybrano: {selectedMarkets.length > 0 ? selectedMarkets.join(', ') : 'brak'}
                  </div>
                </div>
                
                <button 
                  className="action-btn primary"
                  onClick={processKNumbers}
                  disabled={!kNumbers.trim() || selectedMarkets.length === 0 || loading}
                >
                  {loading ? (
                    <>
                      <Loader2 size={18} className="spin" />
                      Pobieranie z PIM...
                    </>
                  ) : (
                    <>
                      <Search size={18} />
                      Pobierz z PIM
                    </>
                  )}
                </button>
              </div>
            </motion.div>
          )}

          {/* Step 2: Review */}
          {step === 2 && parsedData && (
            <motion.div
              key="step2"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className="step-content"
            >
              <div className="review-grid">
                <div className="panel">
                  <div className="panel-header">
                    <h2>Podgląd danych</h2>
                    <span className="badge">{parsedData.totalRows} wierszy</span>
                  </div>
                  
                  <div className="data-table-wrapper">
                    <table className="data-table">
                      <thead>
                        <tr>
                          {parsedData.columns.slice(0, 5).map(col => (
                            <th key={col}>{COLUMN_LABELS[col] || col}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {parsedData.preview.map((row, i) => (
                          <tr key={i}>
                            {parsedData.columns.slice(0, 5).map(col => (
                              <td key={col}>{row[col]}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>

                <div className="panel">
                  <div className="panel-header">
                    <h2>Wykryte rodziny produktów</h2>
                    <span className="badge success">{families.length} rodzin</span>
                  </div>
                  
                  <div className="families-list">
                    {families.map(family => (
                      <div key={family.id} className="family-card">
                        <div className="family-icon">
                          <Layers size={20} />
                        </div>
                        <div className="family-info">
                          <h4>{family.name}</h4>
                          <p>Parent: {family.parentSku}</p>
                          <div className="family-meta">
                            <span>{family.childCount} wariantów</span>
                            <span className="theme-badge">{family.variationTheme}</span>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
                
                {/* SKU Generator Panel */}
                <div className="panel full-width">
                  <div className="panel-header">
                    <h2>🏷️ Generator SKU (item_sku)</h2>
                    <span className="badge">Konfiguracja formatu</span>
                  </div>
                  
                  <div className="sku-generator">
                    <p className="sku-description">
                      Ustaw logikę generowania <strong>item_sku</strong> dla Amazon. 
                      Np. <code>MAG_5903699455531</code> = prefix "MAG_" + EAN
                    </p>
                    
                    <div className="sku-config-form">
                      <div className="sku-field">
                        <label>Źródło (base value):</label>
                        <select 
                          value={skuConfig.source}
                          onChange={(e) => setSkuConfig(prev => ({ ...prev, source: e.target.value }))}
                        >
                          <option value="sku">SKU (domyślne)</option>
                          <option value="ean">EAN (13 cyfr)</option>
                          <option value="k_number">K-Number</option>
                        </select>
                      </div>
                      
                      <div className="sku-field">
                        <label>Prefix (przed):</label>
                        <input 
                          type="text"
                          value={skuConfig.prefix}
                          onChange={(e) => setSkuConfig(prev => ({ ...prev, prefix: e.target.value }))}
                          placeholder="np. MAG_ lub KADAX-"
                        />
                      </div>
                      
                      <div className="sku-field">
                        <label>Suffix (po):</label>
                        <input 
                          type="text"
                          value={skuConfig.suffix}
                          onChange={(e) => setSkuConfig(prev => ({ ...prev, suffix: e.target.value }))}
                          placeholder="np. _DE lub -AMZ"
                        />
                      </div>
                      
                      <div className="sku-field">
                        <label>Zamień (opcjonalne):</label>
                        <div className="replace-fields">
                          <input 
                            type="text"
                            value={skuConfig.replace_from}
                            onChange={(e) => setSkuConfig(prev => ({ ...prev, replace_from: e.target.value }))}
                            placeholder="z..."
                          />
                          <span>→</span>
                          <input 
                            type="text"
                            value={skuConfig.replace_to}
                            onChange={(e) => setSkuConfig(prev => ({ ...prev, replace_to: e.target.value }))}
                            placeholder="na..."
                          />
                        </div>
                      </div>
                    </div>
                    
                    {/* Live Preview */}
                    <div className="sku-preview">
                      <h4>📋 Podgląd (na podstawie pierwszych produktów):</h4>
                      <div className="sku-preview-list">
                        {parsedData?.preview?.slice(0, 3).map((p, idx) => {
                          // EAN może być w p.EAN lub w p.rawData.ean
                          const eanValue = p.EAN || p.rawData?.ean || p.rawData?.attributes?.ean || p.rawData?.attributes?.gtin || '';
                          const source = skuConfig.source === 'ean' ? eanValue : 
                                        skuConfig.source === 'k_number' ? p.KNumber : 
                                        (p.SKU || p.KNumber);
                          let baseValue = source || (skuConfig.source === 'ean' ? '[brak EAN]' : '');
                          if (skuConfig.replace_from && baseValue) {
                            baseValue = baseValue.replace(skuConfig.replace_from, skuConfig.replace_to);
                          }
                          const generatedSku = `${skuConfig.prefix}${baseValue}${skuConfig.suffix}`;
                          
                          return (
                            <div key={idx} className="sku-preview-row">
                              <span className="sku-source">{source || '[brak wartości]'}</span>
                              <span className="sku-arrow">→</span>
                              <span className="sku-result">{generatedSku}</span>
                              {skuConfig.source === 'ean' && !eanValue && (
                                <span className="sku-warning">⚠️ Brak EAN w PIM</span>
                              )}
                            </div>
                          );
                        })}
                      </div>
                    </div>
                    
                    <button 
                      className="action-btn secondary"
                      onClick={() => {
                        // SKU config is saved locally and will be used when generating draft
                        alert(`✅ SKU config zapisany lokalnie!\n\nPrzykład dla pierwszego produktu:\n${skuConfig.prefix}${
                          skuConfig.source === 'ean' ? (parsedData?.preview?.[0]?.EAN || 'EAN') :
                          skuConfig.source === 'k_number' ? (parsedData?.preview?.[0]?.KNumber || 'K-number') :
                          (parsedData?.preview?.[0]?.SKU || parsedData?.preview?.[0]?.KNumber || 'SKU')
                        }${skuConfig.suffix}\n\nConfig zostanie zastosowany przy generowaniu draftu.`);
                      }}
                    >
                      💾 Zapisz konfigurację SKU
                    </button>
                  </div>
                </div>
              </div>

              <div className="step-actions">
                <button className="action-btn secondary" onClick={() => setStep(1)}>
                  <ArrowLeft size={18} />
                  Wróć
                </button>
                <button className="action-btn primary" onClick={() => setStep(3)}>
                  Dalej: Konfiguracja
                  <Settings size={18} />
                </button>
              </div>
            </motion.div>
          )}

          {/* Step 3: Configure */}
          {step === 3 && (
            <motion.div
              key="step3"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className="step-content"
            >
              <div className="config-grid">
                <div className="panel full-width">
                  <div className="panel-header">
                    <h2>🤖 Wykryte typy produktów</h2>
                    <span className="badge ai-badge">AI Detected</span>
                  </div>
                  <div className="detected-types">
                    {parsedData?.preview?.map((p, i) => (
                      <div key={i} className="detected-type-row">
                        <span className="sku">{p.SKU}</span>
                        <span className="product-name">{p.Title?.substring(0, 40)}...</span>
                        <div className="type-selector">
                          <input
                            type="text"
                            list={`product-types-${i}`}
                            className="type-input"
                            value={p.ProductType}
                            onChange={(e) => {
                              const newData = {...parsedData};
                              newData.preview[i].ProductType = e.target.value.toUpperCase();
                              setParsedData(newData);
                            }}
                            placeholder="Szukaj typu..."
                          />
                          <datalist id={`product-types-${i}`}>
                            {AMAZON_PRODUCT_TYPES.map(type => (
                              <option key={type} value={type} />
                            ))}
                          </datalist>
                        </div>
                        <span className={`status-badge ${p.Status.includes('Ready') ? 'ready' : 'issues'}`}>
                          {p.Status}
                        </span>
                      </div>
                    ))}
                  </div>
                  
                  {/* Validation Issues */}
                  {parsedData?.preview?.some(p => p.Errors) && (
                    <div className="validation-errors">
                      <h4>❌ Błędy blokujące:</h4>
                      {parsedData.preview.filter(p => p.Errors).map((p, i) => (
                        <div key={i} className="error-item">
                          <strong>{p.SKU}:</strong> {p.Errors.split(',').map((err, j) => (
                            <span key={j} className="error-tag">
                              {err.includes('EAN') ? '🔢 Brak EAN/GTIN' :
                               err.includes('image') ? '🖼️ Brak zdjęć w PIM' : err}
                            </span>
                          ))}
                        </div>
                      ))}
                    </div>
                  )}
                  
                  {/* Validation Warnings */}
                  {parsedData?.preview?.some(p => p.Warnings) && (
                    <div className="validation-warnings">
                      <h4>⚠️ Ostrzeżenia (AI uzupełni):</h4>
                      {parsedData.preview.filter(p => p.Warnings).map((p, i) => (
                        <div key={i} className="warning-item">
                          <strong>{p.SKU}:</strong> {p.Warnings.split(',').map((warn, j) => (
                            <span key={j} className="warning-tag">{warn}</span>
                          ))}
                        </div>
                      ))}
                    </div>
                  )}
                  
                  {/* PIM Data Quality */}
                  <div className="pim-data-quality">
                    <h4>📦 Dane z PIM:</h4>
                    {parsedData?.preview?.map((p, i) => (
                      <div key={i} className="pim-quality-row">
                        <span className="sku">{p.SKU}</span>
                        <span className={`quality-badge ${p.HasImages ? 'ok' : 'missing'}`}>
                          🖼️ {p.ImageCount || 0} zdjęć
                        </span>
                        <span className={`quality-badge ${p.HasDescription ? 'ok' : 'missing'}`}>
                          📝 {p.HasDescription ? 'Opis ✓' : 'Brak opisu'}
                        </span>
                      </div>
                    ))}
                  </div>
                  
                  <p className="ai-note">
                    <Zap size={14} /> AI wygeneruje tytuły, bullets i opisy na podstawie danych z PIM (polskich opisów, atrybutów)
                  </p>
                </div>

                <div className="panel">
                  <div className="panel-header">
                    <h2>🌍 Wybrane rynki</h2>
                  </div>
                  <div className="selected-markets-display">
                    {selectedMarkets.map(m => {
                      const market = MARKETS.find(mk => mk.code === m);
                      return market ? (
                        <span key={m} className="market-chip">
                          {market.flag} {market.code}
                        </span>
                      ) : null;
                    })}
                  </div>
                </div>

                <div className="panel">
                  <div className="panel-header">
                    <h2>📤 Format eksportu</h2>
                  </div>
                  <div className="export-options">
                    {[
                      { id: 'amazon', name: 'Amazon Flat File', icon: FileSpreadsheet },
                      { id: 'json', name: 'JSON Feed', icon: FileText },
                    ].map(opt => (
                      <button
                        key={opt.id}
                        className={`export-opt ${exportFormat === opt.id ? 'active' : ''}`}
                        onClick={() => setExportFormat(opt.id)}
                      >
                        <opt.icon size={24} />
                        <span>{opt.name}</span>
                      </button>
                    ))}
                  </div>
                </div>

                {/* AI Generation Panel with Live Logs */}
                <div className="panel full-width ai-preview-panel">
                  <div className="panel-header">
                    <h2>🤖 AI Content Generator</h2>
                    <div className="ai-header-actions">
                      <button 
                        className={`btn-toggle-logs ${aiLogsVisible ? 'active' : ''}`}
                        onClick={() => setAiLogsVisible(!aiLogsVisible)}
                      >
                        📋 Logi {aiLogs.length > 0 && `(${aiLogs.length})`}
                      </button>
                      <button className="btn-generate-preview" onClick={async () => {
                        // Start AI generation with live logging
                        setAiPreviewLoading(true);
                        setAiLogsVisible(true);
                        clearAiLogs();
                        
                        const products = parsedData?.preview || [];
                        const markets = selectedMarkets;
                        
                        addAiLog('info', '🚀 Rozpoczynam generowanie treści AI...');
                        addAiLog('info', `📦 Produkty do przetworzenia: ${products.length}`);
                        addAiLog('info', `🌍 Rynki docelowe: ${markets.join(', ')}`);
                        
                        try {
                          for (const product of products) {
                            addAiLog('product', `📦 Przetwarzam: ${product.SKU}`, product.Title);
                            
                            for (const market of markets) {
                              const flag = MARKETS.find(m => m.code === market)?.flag || '';
                              
                              // Simulate steps (in real impl, use SSE for streaming)
                              addAiLog('step', `${flag} ${market}: Buduję tytuł...`);
                              await new Promise(r => setTimeout(r, 300));
                              
                              addAiLog('step', `${flag} ${market}: Generuję bullet points...`);
                              await new Promise(r => setTimeout(r, 200));
                              
                              addAiLog('step', `${flag} ${market}: Tworzę opis produktu...`);
                              await new Promise(r => setTimeout(r, 200));
                              
                              addAiLog('step', `${flag} ${market}: Optymalizuję słowa kluczowe...`);
                              await new Promise(r => setTimeout(r, 100));
                            }
                          }
                          
                          // Call actual API
                          addAiLog('info', '🔄 Wysyłam zapytanie do API...');
                          const res = await fetch(`${API_URL}/api/productonboard/sessions/${sessionId}/generate-content`, {
                            method: 'POST',
                            headers: { 'Content-Type': 'application/json' },
                            body: JSON.stringify({ 
                              target_markets: markets,
                              sku_config: skuConfig // Pass SKU config for item_sku generation
                            })
                          });
                          const data = await res.json();
                          
                          // Log generated content
                          if (data.content) {
                            for (const [kNum, productData] of Object.entries(data.content)) {
                              addAiLog('success', `✅ ${kNum} - wygenerowano treści`);
                              
                              for (const [market, content] of Object.entries(productData.markets || {})) {
                                const flag = MARKETS.find(m => m.code === market)?.flag || '';
                                if (content.title) {
                                  addAiLog('result', `${flag} Tytuł: "${content.title.substring(0, 60)}..."`, 
                                    `${content.title.length} znaków`);
                                }
                                if (content.bullets?.length) {
                                  addAiLog('result', `${flag} Bullets: ${content.bullets.length} punktów`);
                                }
                              }
                            }
                          }
                          
                          setAiPreview(data);
                          addAiLog('success', '🎉 Generowanie zakończone!');
                          
                          // AUTO-NAVIGATE to Content Review after AI finishes
                          if (data.content && Object.keys(data.content).length > 0) {
                            addAiLog('info', '➡️ Przechodzę do przeglądu treści...');
                            setTimeout(() => {
                              // Initialize editable content from AI preview
                              const content = {};
                              for (const [kNum, productData] of Object.entries(data.content)) {
                                const pimProduct = parsedData?.preview?.find(p => p.KNumber === kNum)?.rawData;
                                const images = productData.source_data?.images || pimProduct?.images || [];
                                
                                // Clean pim_name from UUID prefix
                                const rawPimName = pimProduct?.name || '';
                                const cleanPimName = cleanProductName(rawPimName);
                                
                                content[kNum] = {
                                  sku: productData.sku || pimProduct?.sku || kNum,
                                  product_type: productData.product_type || 'HOME',
                                  product_type_confidence: productData.product_type_confidence || 0,
                                  product_type_source: productData.product_type_source || 'keyword',
                                  images: images,
                                  main_image: images[0] || '',
                                  pim_name: cleanPimName,
                                  pim_description: pimProduct?.description || '',
                                  ...productData.markets
                                };
                                for (const market of selectedMarkets) {
                                  setMarketApproval(kNum, market, 'pending');
                                }
                              }
                              setEditableContent(content);
                              setActiveProduct(Object.keys(content)[0]);
                              setActiveMarket(selectedMarkets[0]);
                              setStep(4);
                            }, 1000);
                          }
                          
                        } catch (err) {
                          addAiLog('error', `❌ Błąd: ${err.message}`);
                          console.error('AI generation error:', err);
                        }
                        
                        setAiPreviewLoading(false);
                      }}>
                        {aiPreviewLoading ? <Loader2 size={16} className="spin" /> : <Zap size={16} />}
                        {aiPreviewLoading ? 'Generuję...' : 'Generuj treści AI'}
                      </button>
                    </div>
                  </div>
                  
                  {/* AI Logs Panel */}
                  {aiLogsVisible && (
                    <div className="ai-logs-panel">
                      <div className="ai-logs-header">
                        <span>📋 Logi generowania AI</span>
                        <button className="btn-clear-logs" onClick={clearAiLogs}>Wyczyść</button>
                      </div>
                      <div className="ai-logs-content">
                        {aiLogs.length === 0 ? (
                          <div className="ai-logs-empty">
                            Kliknij "Generuj treści AI" aby rozpocząć...
                          </div>
                        ) : (
                          aiLogs.map((log, i) => (
                            <div key={i} className={`ai-log-entry ${log.type}`}>
                              <span className="log-time">{log.timestamp}</span>
                              <span className="log-message">{log.message}</span>
                              {log.details && <span className="log-details">{log.details}</span>}
                            </div>
                          ))
                        )}
                      </div>
                    </div>
                  )}
                  
                  {/* Generated Content Preview */}
                  <div className="ai-preview-content">
                    {!aiPreview ? (
                      <div className="ai-preview-empty">
                        <p>Kliknij "Generuj treści AI" aby wygenerować treści dla Amazon</p>
                        <ul className="ai-will-generate">
                          <li>📝 <strong>Tytuły</strong> - MARKA na początku, max 199 znaków, bez przecinków</li>
                          <li>🔸 <strong>Bullet Points</strong> - 5 punktów z korzyściami produktu</li>
                          <li>📄 <strong>Opisy</strong> - A+ content w języku rynku docelowego</li>
                          <li>🏷️ <strong>Słowa kluczowe</strong> - backend search terms (250 znaków)</li>
                        </ul>
                        <p className="ai-source-note">
                          ℹ️ AI generuje treści na podstawie polskich opisów i atrybutów z PIM
                        </p>
                      </div>
                    ) : (
                      <div className="ai-preview-results">
                        {Object.entries(aiPreview.content || {}).map(([kNum, productData]) => (
                          <div key={kNum} className="product-content-preview">
                            <h4>📦 {kNum} - {productData.sku}</h4>
                            <div className="markets-content">
                              {Object.entries(productData.markets || {}).map(([market, content]) => (
                                <div key={market} className="market-content-preview">
                                  <h5>{MARKETS.find(m => m.code === market)?.flag} {market}</h5>
                                  {content.title && (
                                    <div className="preview-field">
                                      <label>Tytuł ({content.title.length}/199):</label>
                                      <code className={content.title.length > 199 ? 'too-long' : ''}>{content.title}</code>
                                    </div>
                                  )}
                                  {content.bullets && content.bullets.length > 0 && (
                                    <div className="preview-field">
                                      <label>Bullet Points ({content.bullets.length}/5):</label>
                                      <ul className="bullets-preview">
                                        {content.bullets.map((b, i) => <li key={i}>{b}</li>)}
                                      </ul>
                                    </div>
                                  )}
                                  {content.description && (
                                    <div className="preview-field">
                                      <label>Opis ({content.description.length} znaków):</label>
                                      <p className="description-preview">{content.description.substring(0, 200)}...</p>
                                    </div>
                                  )}
                                  {content.keywords && (
                                    <div className="preview-field">
                                      <label>Keywords ({content.keywords.length}/250):</label>
                                      <code className="keywords-preview">{content.keywords}</code>
                                    </div>
                                  )}
                                  {content.error && (
                                    <div className="preview-field error">
                                      <label>⚠️ Fallback użyty:</label>
                                      <span>{content.error}</span>
                                    </div>
                                  )}
                                </div>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              </div>

              <div className="step-actions">
                <button className="action-btn secondary" onClick={() => setStep(2)}>
                  <ArrowLeft size={18} />
                  Wróć
                </button>
                {/* Block button until AI generates content */}
                {!aiPreview?.content ? (
                  <div className="ai-required-notice">
                    <Zap size={18} />
                    <span>Najpierw wygeneruj treści AI (przycisk powyżej)</span>
                  </div>
                ) : (
                  <button 
                    className="action-btn primary success" 
                    onClick={() => {
                      // Content already initialized by auto-navigate, but allow manual if needed
                      if (Object.keys(editableContent).length === 0 && aiPreview?.content) {
                        const content = {};
                        for (const [kNum, productData] of Object.entries(aiPreview.content)) {
                          const pimProduct = parsedData?.preview?.find(p => p.KNumber === kNum)?.rawData;
                          const images = productData.source_data?.images || pimProduct?.images || [];
                          
                          // Clean pim_name from UUID prefix
                          const rawPimName = pimProduct?.name || '';
                          const cleanPimNameValue = cleanProductName(rawPimName);
                          
                          content[kNum] = {
                            sku: productData.sku || pimProduct?.sku || kNum,
                            product_type: productData.product_type || 'HOME',
                            product_type_confidence: productData.product_type_confidence || 0,
                            product_type_source: productData.product_type_source || 'keyword',
                            // Amazon category (browse tree)
                            browse_node_id: productData.browse_node_id || null,
                            browse_path: productData.browse_path || null,
                            images: images,
                            main_image: images[0] || '',
                            pim_name: cleanPimNameValue,
                            pim_description: pimProduct?.description || '',
                            ...productData.markets
                          };
                          for (const market of selectedMarkets) {
                            setMarketApproval(kNum, market, 'pending');
                          }
                        }
                        setEditableContent(content);
                        setActiveProduct(Object.keys(content)[0]);
                        setActiveMarket(selectedMarkets[0]);
                      }
                      setStep(4);
                    }}
                    disabled={loading || aiPreviewLoading}
                  >
                    <CheckCircle size={18} />
                    ✅ Treści gotowe - Przejdź do edycji
                  </button>
                )}
              </div>
            </motion.div>
          )}

          {/* Step 4: Content Review & Edit */}
          {step === 4 && (
            <motion.div
              key="step4"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className="step-content content-review-step"
            >
              <div className="content-review-layout">
                {/* Product Selector Sidebar */}
                <div className="product-sidebar">
                  <h3>📦 Produkty ({Object.keys(editableContent).length})</h3>
                  <div className="product-list">
                    {Object.keys(editableContent).length === 0 ? (
                      <div className="empty-list">
                        <p>Brak produktów do wyświetlenia</p>
                        <button className="action-btn secondary small" onClick={() => setStep(3)}>
                          ← Wróć do konfiguracji
                        </button>
                      </div>
                    ) : (
                      Object.entries(editableContent).map(([kNum, data]) => {
                        const allApproved = selectedMarkets.every(m => 
                          marketStatus[`${kNum}_${m}`] === 'approved'
                        );
                        const hasIssues = selectedMarkets.some(m => 
                          marketStatus[`${kNum}_${m}`] === 'needs-edit'
                        );
                        const imgCount = data.images?.length || 0;
                        return (
                          <button
                            key={kNum}
                            className={`product-item ${activeProduct === kNum ? 'active' : ''} ${allApproved ? 'approved' : ''} ${hasIssues ? 'issues' : ''}`}
                            onClick={() => setActiveProduct(kNum)}
                          >
                            <div className="product-item-thumb">
                              {imgCount > 0 ? (
                                <img src={getImageUrl(data.images[0])} alt="" />
                              ) : (
                                <span className="no-thumb">📦</span>
                              )}
                            </div>
                            <div className="product-item-info">
                              <span className="product-sku">{data.sku || kNum}</span>
                              <span className="product-meta">{imgCount} 🖼️ • {data.product_type}</span>
                            </div>
                            <span className="product-status">
                              {allApproved ? '✅' : hasIssues ? '⚠️' : '⏳'}
                            </span>
                          </button>
                        );
                      })
                    )}
                  </div>
                </div>

                {/* Main Content Area - Split View */}
                <div className="content-main">
                  {activeProduct && editableContent[activeProduct] ? (
                    <div className="content-split-view">
                      {/* Left: Amazon Preview */}
                      <div className="amazon-preview-panel">
                        <div className="preview-header">
                          <h3>👁️ Podgląd Amazon</h3>
                          <div className="preview-market-selector">
                            {selectedMarkets.map(market => {
                              const flag = MARKETS.find(m => m.code === market)?.flag;
                              return (
                                <button
                                  key={market}
                                  className={`preview-market-btn ${activeMarket === market ? 'active' : ''}`}
                                  onClick={() => setActiveMarket(market)}
                                >
                                  {flag}
                                </button>
                              );
                            })}
                          </div>
                        </div>
                        
                        {/* Amazon-style Product Card */}
                        <div className="amazon-product-card">
                          {/* Main Image */}
                          <div className="amazon-images">
                            <div className="main-image-container">
                              {editableContent[activeProduct].images?.length > 0 ? (
                                <img 
                                  src={getImageUrl(editableContent[activeProduct].images[0])} 
                                  alt="Main product" 
                                  className="main-product-image"
                                />
                              ) : (
                                <div className="no-image-placeholder">
                                  <Package size={64} />
                                  <p>Brak zdjęć w PIM</p>
                                </div>
                              )}
                              {editableContent[activeProduct].images?.length > 0 && (
                                <div className="hero-badge">HERO</div>
                              )}
                            </div>
                            {/* Thumbnail strip with drag & drop */}
                            <div className="thumbnail-strip">
                              {(editableContent[activeProduct].images || []).slice(0, 8).map((img, idx) => (
                                <div 
                                  key={idx} 
                                  className={`thumbnail ${idx === 0 ? 'active hero' : ''} ${editableContent[activeProduct]?.swatch_url === img ? 'swatch' : ''}`}
                                  draggable
                                  onDragStart={(e) => e.dataTransfer.setData('imageIndex', idx.toString())}
                                  onDragOver={(e) => e.preventDefault()}
                                  onDrop={(e) => {
                                    e.preventDefault();
                                    const fromIndex = parseInt(e.dataTransfer.getData('imageIndex'));
                                    if (fromIndex !== idx) {
                                      moveImage(activeProduct, fromIndex, idx);
                                    }
                                  }}
                                  title={idx === 0 ? 'HERO - Zdjęcie główne' : `Zdjęcie ${idx + 1} - Przeciągnij aby zmienić kolejność`}
                                >
                                  <img src={getImageUrl(img)} alt={`Thumbnail ${idx + 1}`} />
                                  <span className="slot-label">{idx === 0 ? 'MAIN' : `PT0${idx + 1}`}</span>
                                </div>
                              ))}
                            </div>
                            {/* Image Actions */}
                            <div className="image-actions">
                              <button 
                                className="image-action-btn"
                                onClick={() => {
                                  // Swap first two images to change HERO
                                  if (editableContent[activeProduct]?.images?.length >= 2) {
                                    moveImage(activeProduct, 1, 0);
                                  }
                                }}
                                disabled={!editableContent[activeProduct]?.images?.length || editableContent[activeProduct].images.length < 2}
                              >
                                🔄 Zmień HERO
                              </button>
                              <button 
                                className="image-action-btn swatch"
                                onClick={async () => {
                                  // Generate AI swatch
                                  addAiLog('info', `🎨 Generuję swatch dla ${activeProduct}...`);
                                  try {
                                    const res = await fetch(`${API_URL}/api/productonboard/sessions/${sessionId}/products/${activeProduct}/images/swatch`, {
                                      method: 'POST',
                                      headers: { 'Content-Type': 'application/json' },
                                      body: JSON.stringify({
                                        sku: editableContent[activeProduct]?.sku || activeProduct,
                                        generate_ai_swatch: true
                                      })
                                    });
                                    if (res.ok) {
                                      const data = await res.json();
                                      if (data.swatch) {
                                        addAiLog('success', `✅ Swatch wygenerowany!`);
                                        setEditableContent(prev => ({
                                          ...prev,
                                          [activeProduct]: {
                                            ...prev[activeProduct],
                                            swatch_url: data.swatch.url,
                                          }
                                        }));
                                      } else if (data.status === 'failed') {
                                        addAiLog('error', `❌ Swatch: ${data.message}`);
                                      }
                                    } else {
                                      const errData = await res.json().catch(() => ({}));
                                      addAiLog('error', `❌ Błąd API: ${errData.detail || res.statusText}`);
                                    }
                                  } catch (e) {
                                    addAiLog('error', `❌ Błąd generowania swatch: ${e.message}`);
                                  }
                                }}
                              >
                                ✨ AI Swatch
                              </button>
                            </div>
                            
                            {/* Swatch Selection - User can pick from images */}
                            {editableContent[activeProduct]?.images?.length > 0 && (
                              <div className="swatch-selector">
                                <label>🎨 Wybierz swatch (kolor/tekstura zbliżenie):</label>
                                <p className="swatch-hint">Swatch to zdjęcie pokazujące kolor/fakturę produktu z bliska. Wybierz odpowiednie zdjęcie lub usuń jeśli niepotrzebny.</p>
                                <div className="swatch-options">
                                  {editableContent[activeProduct].images.slice(0, 8).map((img, idx) => (
                                    <div 
                                      key={idx}
                                      className={`swatch-option ${editableContent[activeProduct].swatch_url === img ? 'selected' : ''}`}
                                      onClick={() => {
                                        setEditableContent(prev => ({
                                          ...prev,
                                          [activeProduct]: {
                                            ...prev[activeProduct],
                                            swatch_url: img,
                                          }
                                        }));
                                        addAiLog('info', `🎨 Swatch zmieniony na zdjęcie ${idx + 1}`);
                                      }}
                                      title={`Użyj zdjęcie ${idx + 1} jako swatch`}
                                    >
                                      <img src={getImageUrl(img)} alt={`Swatch option ${idx + 1}`} />
                                    </div>
                                  ))}
                                </div>
                                <div className="swatch-actions">
                                  {editableContent[activeProduct].swatch_url && (
                                    <>
                                      <span className="swatch-selected">✓ Swatch wybrany</span>
                                      <button 
                                        className="swatch-remove-btn"
                                        onClick={() => {
                                          setEditableContent(prev => ({
                                            ...prev,
                                            [activeProduct]: {
                                              ...prev[activeProduct],
                                              swatch_url: null,
                                            }
                                          }));
                                          addAiLog('info', `🗑️ Swatch usunięty`);
                                        }}
                                        title="Usuń swatch"
                                      >
                                        🗑️ Usuń swatch
                                      </button>
                                    </>
                                  )}
                                  <label className="swatch-upload-btn" title="Wgraj własne zdjęcie jako swatch">
                                    📤 Wgraj własny
                                    <input 
                                      type="file" 
                                      accept="image/*" 
                                      style={{display: 'none'}}
                                      onChange={async (e) => {
                                        const file = e.target.files?.[0];
                                        if (file) {
                                          // Convert to base64 or upload
                                          const reader = new FileReader();
                                          reader.onload = () => {
                                            setEditableContent(prev => ({
                                              ...prev,
                                              [activeProduct]: {
                                                ...prev[activeProduct],
                                                swatch_url: reader.result,
                                                swatch_custom: true,
                                              }
                                            }));
                                            addAiLog('success', `📤 Wgrano własny swatch`);
                                          };
                                          reader.readAsDataURL(file);
                                        }
                                      }}
                                    />
                                  </label>
                                </div>
                              </div>
                            )}
                          </div>
                          
                          {/* Product Info */}
                          <div className="amazon-info">
                            {/* Title */}
                            <h1 className="amazon-title">
                              {ensureBrandInTitle(
                                editableContent[activeProduct]?.[activeMarket]?.title || 
                                editableContent[activeProduct]?.pim_name || 
                                'Produkt'
                              )}
                            </h1>
                            
                            {/* Brand */}
                            <p className="amazon-brand">
                              Marka: <a href="#">KADAX</a>
                            </p>
                            
                            {/* Bullets */}
                            <div className="amazon-bullets">
                              <h4>O produkcie:</h4>
                              <ul>
                                {(editableContent[activeProduct]?.[activeMarket]?.bullets || []).map((bullet, idx) => (
                                  bullet ? <li key={idx}>{bullet}</li> : null
                                )).filter(Boolean)}
                                {(!editableContent[activeProduct]?.[activeMarket]?.bullets?.some(b => b)) && (
                                  <li className="placeholder">Bullet points zostaną wygenerowane przez AI...</li>
                                )}
                              </ul>
                            </div>
                            
                            {/* Description Preview */}
                            <div className="amazon-description">
                              <h4>Opis produktu:</h4>
                              <p>
                                {editableContent[activeProduct]?.[activeMarket]?.description?.substring(0, 300) || 
                                 editableContent[activeProduct]?.pim_description?.substring(0, 300) || 
                                 'Opis zostanie wygenerowany przez AI...'}
                                {(editableContent[activeProduct]?.[activeMarket]?.description?.length > 300) && '...'}
                              </p>
                            </div>
                          </div>
                        </div>
                        
                        {/* Image count badge */}
                        <div className="image-count-badge">
                          🖼️ {editableContent[activeProduct].images?.length || 0} zdjęć z PIM
                        </div>
                      </div>
                      
                      {/* Right: Editor Panel */}
                      <div className="editor-panel">
                        <div className="editor-header">
                          <h3>✏️ Edytuj treści</h3>
                          <div className="product-type-badges">
                            <span className={`product-type-badge ${editableContent[activeProduct].product_type_confidence < 0.5 ? 'low-confidence' : ''}`} 
                                  title={`Wykryto przez: ${editableContent[activeProduct].product_type_source === 'ai-pro' ? 'GPT-5.2 Pro' : editableContent[activeProduct].product_type_source || 'keyword'}, Pewność: ${Math.round((editableContent[activeProduct].product_type_confidence || 0) * 100)}%`}>
                              📦 {editableContent[activeProduct].product_type}
                              {editableContent[activeProduct].product_type_source === 'ai-pro' && ' 🧠'}
                              {editableContent[activeProduct].product_type_source === 'ai' && ' 🤖'}
                              {editableContent[activeProduct].product_type_confidence < 0.5 && ' ⚠️'}
                            </span>
                            {editableContent[activeProduct].browse_node_id && (
                              <span className="browse-node-badge" 
                                    title={editableContent[activeProduct].browse_path || 'Amazon Category'}>
                                🗂️ Node: {editableContent[activeProduct].browse_node_id}
                              </span>
                            )}
                          </div>
                        </div>
                        
                        {/* Market Tabs */}
                        <div className="market-tabs">
                          {selectedMarkets.map(market => {
                            const status = marketStatus[`${activeProduct}_${market}`];
                            const flag = MARKETS.find(m => m.code === market)?.flag;
                            return (
                              <button
                                key={market}
                                className={`market-tab ${activeMarket === market ? 'active' : ''} ${status}`}
                                onClick={() => setActiveMarket(market)}
                              >
                                {flag} {market}
                                {status === 'approved' && ' ✓'}
                              </button>
                            );
                          })}
                        </div>

                        {/* Market Content Editor */}
                        {activeMarket && (
                          <div className="market-content-editor">
                            {/* Title */}
                            <div className="field-group">
                              <label>
                                📝 Tytuł 
                                <span className={`char-count ${(editableContent[activeProduct]?.[activeMarket]?.title || '').length > 199 ? 'error' : ''}`}>
                                  {(editableContent[activeProduct]?.[activeMarket]?.title || '').length}/199
                                </span>
                              </label>
                              <textarea
                                className={`field-input title-input ${(editableContent[activeProduct]?.[activeMarket]?.title || '').length > 199 ? 'error' : ''}`}
                                value={editableContent[activeProduct]?.[activeMarket]?.title || ''}
                                onChange={(e) => updateContent(activeProduct, activeMarket, 'title', e.target.value)}
                                placeholder="Tytuł produktu na Amazon..."
                                rows={2}
                              />
                            </div>

                            {/* Bullets */}
                            <div className="field-group">
                              <label>🔸 Bullet Points</label>
                              <div className="bullets-editor">
                                {[0, 1, 2, 3, 4].map((idx) => (
                                  <div key={idx} className="bullet-row">
                                    <span className="bullet-num">{idx + 1}</span>
                                    <textarea
                                      className="field-input bullet-input"
                                      value={(editableContent[activeProduct]?.[activeMarket]?.bullets || [])[idx] || ''}
                                      onChange={(e) => updateBullet(activeProduct, activeMarket, idx, e.target.value)}
                                      placeholder={`Bullet point ${idx + 1}...`}
                                      rows={2}
                                    />
                                  </div>
                                ))}
                              </div>
                            </div>

                            {/* Description */}
                            <div className="field-group">
                              <label>
                                📄 Opis 
                                <span className="char-count">
                                  {(editableContent[activeProduct]?.[activeMarket]?.description || '').length} znaków
                                </span>
                              </label>
                              <textarea
                                className="field-input description-input"
                                value={editableContent[activeProduct]?.[activeMarket]?.description || ''}
                                onChange={(e) => updateContent(activeProduct, activeMarket, 'description', e.target.value)}
                                placeholder="Opis produktu..."
                                rows={5}
                              />
                            </div>

                            {/* Keywords Section - Backend Search Terms (generic_keyword) */}
                            <div className="keywords-section">
                              <div className="field-group">
                                <label>
                                  🔑 Backend Search Terms (generic_keyword)
                                  <span className={`char-count ${(editableContent[activeProduct]?.[activeMarket]?.keywords || editableContent[activeProduct]?.[activeMarket]?.generic_keyword || '').length > 250 ? 'error' : ''}`}>
                                    {(editableContent[activeProduct]?.[activeMarket]?.keywords || editableContent[activeProduct]?.[activeMarket]?.generic_keyword || '').length}/250 bajtów
                                  </span>
                                  <span className="keyword-hint" title="Ukryte słowa kluczowe dla wyszukiwarki Amazon. Synonimy, warianty pisowni, powiązane terminy. NIE powtarzaj słów z tytułu i bullet points!">ⓘ</span>
                                </label>
                                <textarea
                                  className={`field-input keywords-input ${(editableContent[activeProduct]?.[activeMarket]?.keywords || '').length > 250 ? 'error' : ''}`}
                                  value={editableContent[activeProduct]?.[activeMarket]?.keywords || editableContent[activeProduct]?.[activeMarket]?.generic_keyword || ''}
                                  onChange={(e) => updateContent(activeProduct, activeMarket, 'keywords', e.target.value)}
                                  placeholder="synonimy warianty pisowni powiązane terminy zastosowania..."
                                  rows={3}
                                />
                                <div className="backend-keywords-tip">
                                  💡 <strong>NIE POWTARZAJ</strong> słów z tytułu/bullet points. Dodaj: synonimy, błędy literowe, regionalne warianty, powiązane kategorie. Bez przecinków, tylko spacje.
                                </div>
                              </div>
                            </div>

                            {/* A+ Content Panel - Always show with generate option */}
                            <details className="aplus-content-panel" open={!!editableContent[activeProduct]?.[activeMarket]?.aplus_content}>
                              <summary>
                                ✨ A+ Content (Enhanced Brand Content)
                                <span className="aplus-status">
                                  {editableContent[activeProduct]?.[activeMarket]?.aplus_content?.modules?.length || 0} modułów
                                </span>
                              </summary>
                              {editableContent[activeProduct]?.[activeMarket]?.aplus_content?.modules?.length > 0 ? (
                                <div className="aplus-modules">
                                  {(editableContent[activeProduct][activeMarket].aplus_content.modules || []).map((module, idx) => {
                                    // Extract content from nested Amazon API format
                                    const moduleType = module.contentModuleType || module.type || module.module_type;
                                    const moduleName = moduleType?.replace('STANDARD_', '')?.replace(/_/g, ' ') || `Moduł ${idx + 1}`;
                                    
                                    // Parse module content based on type
                                    let headline = '';
                                    let body = '';
                                    let items = [];
                                    
                                    if (module.standardHeaderImageText) {
                                      headline = module.standardHeaderImageText.headline?.value || '';
                                    } else if (module.standardFourImageText) {
                                      items = module.standardFourImageText.fourImageText || [];
                                    } else if (module.standardSingleImageHighlights) {
                                      headline = module.standardSingleImageHighlights.headline?.value || '';
                                      items = module.standardSingleImageHighlights.highlightedTextBlock || [];
                                    } else if (module.standardTechSpecs) {
                                      items = module.standardTechSpecs.specificationList || [];
                                    } else if (module.standardText) {
                                      headline = module.standardText.headline?.value || '';
                                      body = module.standardText.body?.value || '';
                                    }
                                    
                                    // Fallback for direct properties
                                    if (!headline) headline = module.headline || '';
                                    if (!body) body = module.body || '';
                                    
                                    return (
                                    <div key={idx} className="aplus-module">
                                      <div className="module-header">
                                        <span className="module-type">{moduleType}</span>
                                        <span className="module-name">{moduleName}</span>
                                      </div>
                                      {headline && (
                                        <div className="module-headline">📢 {headline}</div>
                                      )}
                                      {body && (
                                        <div className="module-body">{body.substring(0, 200)}{body.length > 200 && '...'}</div>
                                      )}
                                      {items.length > 0 && (
                                        <div className="module-items">
                                          {items.slice(0, 4).map((item, ii) => (
                                            <div key={ii} className="module-item">
                                              <span className="item-headline">{item.headline?.value || item.headline || item.label || ''}</span>
                                              <span className="item-body">{item.body?.value || item.description?.value || item.value || ''}</span>
                                            </div>
                                          ))}
                                          {items.length > 4 && <span className="more-items">+{items.length - 4} więcej...</span>}
                                        </div>
                                      )}
                                    </div>
                                    );
                                  })}
                                </div>
                              ) : (
                                <div className="aplus-empty">
                                  <p>🚀 A+ Content nie został jeszcze wygenerowany.</p>
                                  <button 
                                    className="action-btn secondary small"
                                    disabled={aplusGenerating}
                                    onClick={async () => {
                                      setAplusGenerating(true);
                                      addAiLog('info', `✨ Generuję A+ Content dla ${activeProduct} (${activeMarket})...`);
                                      try {
                                        const res = await fetch(`${API_URL}/api/productonboard/sessions/${sessionId}/generate-aplus`, {
                                          method: 'POST',
                                          headers: { 'Content-Type': 'application/json' },
                                          body: JSON.stringify({
                                            k_numbers: [activeProduct],
                                            target_markets: [activeMarket],
                                            max_modules: 5
                                          })
                                        });
                                        if (res.ok) {
                                          const data = await res.json();
                                          // API returns aplus_content[k_number].markets[market]
                                          const aplusData = data.aplus_content?.[activeProduct]?.markets?.[activeMarket];
                                          if (aplusData && aplusData.success) {
                                            setEditableContent(prev => ({
                                              ...prev,
                                              [activeProduct]: {
                                                ...prev[activeProduct],
                                                [activeMarket]: {
                                                  ...prev[activeProduct]?.[activeMarket],
                                                  aplus_content: aplusData
                                                }
                                              }
                                            }));
                                            addAiLog('success', `✅ A+ Content wygenerowany! (${aplusData.module_count} modułów)`);
                                          } else {
                                            addAiLog('error', `❌ A+ błąd: ${aplusData?.error || 'Brak danych'}`);
                                          }
                                        } else {
                                          const errorData = await res.json().catch(() => ({}));
                                          addAiLog('error', `❌ Błąd generowania A+: ${errorData.detail || res.statusText}`);
                                        }
                                      } catch (e) {
                                        addAiLog('error', `❌ Błąd: ${e.message}`);
                                      } finally {
                                        setAplusGenerating(false);
                                      }
                                    }}
                                  >
                                    {aplusGenerating ? '⏳ Generowanie...' : '✨ Generuj A+ Content'}
                                  </button>
                                </div>
                              )}
                              <p className="aplus-note">
                                💡 A+ Content wymaga Amazon Brand Registry. 
                                Podgląd pełnej wersji dostępny po eksporcie.
                              </p>
                            </details>

                            {/* QA Verification Status */}
                            {editableContent[activeProduct]?.[activeMarket]?.qa_verification && (
                              <div className="qa-verification-panel">
                                <div className="qa-header">
                                  <h4>
                                    🔍 Weryfikacja jakości
                                    {editableContent[activeProduct][activeMarket].qa_verification.status === 'passed' && (
                                      <span className="qa-badge passed">✓ OK</span>
                                    )}
                                    {editableContent[activeProduct][activeMarket].qa_verification.status === 'needs_revision' && (
                                      <span className="qa-badge warning">⚠️ Wymaga poprawek</span>
                                    )}
                                    {editableContent[activeProduct][activeMarket].qa_verification.status === 'rejected' && (
                                      <span className="qa-badge error">❌ Odrzucone</span>
                                    )}
                                  </h4>
                                  <div className="qa-score-container">
                                    <span className="qa-score" title={editableContent[activeProduct][activeMarket].qa_verification.score_explanation || ''}>
                                      Score: {Math.round(editableContent[activeProduct][activeMarket].qa_verification.score || 0)}/100
                                    </span>
                                    {editableContent[activeProduct][activeMarket].qa_verification.score_explanation && (
                                      <span className="qa-score-explanation">
                                        {editableContent[activeProduct][activeMarket].qa_verification.score_explanation}
                                      </span>
                                    )}
                                  </div>
                                </div>
                                
                                {editableContent[activeProduct][activeMarket].qa_verification.issues_count > 0 && (
                                  <div className="qa-issues">
                                    <div className="qa-issues-summary">
                                      🚨 {editableContent[activeProduct][activeMarket].qa_verification.critical_issues || 0} krytycznych, 
                                      {' '}⚠️ {(editableContent[activeProduct][activeMarket].qa_verification.issues_count || 0) - 
                                        (editableContent[activeProduct][activeMarket].qa_verification.critical_issues || 0)} innych
                                    </div>
                                    {/* Detailed issues list */}
                                    {editableContent[activeProduct][activeMarket].qa_verification.issues?.length > 0 && (
                                      <div className="qa-issues-detail">
                                        <div className="qa-issue-list">
                                          {editableContent[activeProduct][activeMarket].qa_verification.issues.map((issue, idx) => (
                                            <div key={idx} className={`qa-issue-item ${issue.severity || ''}`}>
                                              <span className="issue-icon">{issue.severity === 'critical' ? '🚨' : '⚠️'}</span>
                                              <div className="issue-content">
                                                <span className="issue-field">{issue.field}</span>
                                                <span className="issue-message">{issue.message}</span>
                                                {issue.details && <span className="issue-details">{issue.details}</span>}
                                              </div>
                                            </div>
                                          ))}
                                        </div>
                                      </div>
                                    )}
                                  </div>
                                )}
                                
                                <div className="qa-scores">
                                  <div className="qa-score-item">
                                    <span>Język</span>
                                    <div className="score-bar" style={{width: `${editableContent[activeProduct][activeMarket].qa_verification.language_score || 0}%`}}></div>
                                  </div>
                                  <div className="qa-score-item">
                                    <span>Zgodność</span>
                                    <div className="score-bar" style={{width: `${editableContent[activeProduct][activeMarket].qa_verification.compliance_score || 0}%`}}></div>
                                  </div>
                                </div>
                              </div>
                            )}

                            {/* Amazon Attributes - FULLY EDITABLE */}
                            {editableContent[activeProduct]?.[activeMarket]?.amazon_attributes && (
                              <details className="amazon-attributes-panel expanded-panel" open={expandedAttributes}>
                                <summary onClick={(e) => { e.preventDefault(); setExpandedAttributes(!expandedAttributes); }}>
                                  📦 Atrybuty Amazon 
                                  <span className="attr-count">
                                    ({editableContent[activeProduct][activeMarket].amazon_attributes.total || 0} atrybutów)
                                  </span>
                                  {editableContent[activeProduct][activeMarket].amazon_attributes.is_complete ? (
                                    <span className="attr-status complete">✓ Kompletne</span>
                                  ) : (
                                    <span className="attr-status incomplete">⚠️ Brakujące: {
                                      (editableContent[activeProduct][activeMarket].amazon_attributes.missing_required || []).join(', ')
                                    }</span>
                                  )}
                                  <span className="expand-toggle">{expandedAttributes ? '▼ Zwiń' : '▶ Rozwiń'}</span>
                                </summary>
                                
                                {/* Helper function for attribute editing */}
                                {(() => {
                                  const allAttributes = editableContent[activeProduct][activeMarket].amazon_attributes.attributes || [];
                                  
                                  // Group attributes logically
                                  const identifiers = allAttributes.filter(a => 
                                    ['seller_sku', 'item_sku', 'external_product_id', 'external_product_id_type'].includes(a.name)
                                  );
                                  const bulletPoints = allAttributes.filter(a => a.name.startsWith('bullet_point'));
                                  const physicalAttrs = allAttributes.filter(a => 
                                    ['color', 'color_map', 'material', 'material_type', 'size', 'item_weight', 'capacity',
                                     'item_dimensions_length', 'item_dimensions_width', 'item_dimensions_height'].includes(a.name)
                                  );
                                  const specialFeatures = allAttributes.filter(a => a.name.startsWith('special_feature'));
                                  const otherAttrs = allAttributes.filter(a => {
                                    const isGrouped = identifiers.includes(a) || bulletPoints.includes(a) || 
                                                      physicalAttrs.includes(a) || specialFeatures.includes(a);
                                    const isListing = ['standard_price', 'quantity', 'condition_type', 'update_delete'].includes(a.name);
                                    return !isGrouped && !isListing;
                                  });
                                  
                                  // Render editable attribute
                                  const renderEditableAttr = (attr, idx, groupKey) => {
                                    const isUUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(String(attr.value || ''));
                                    if (isUUID) return null;
                                    
                                    return (
                                      <div key={`${groupKey}-${idx}`} className={`attr-item editable source-${attr.source} ${attr.required ? 'required' : ''}`}>
                                        <div className="attr-header">
                                          <span className="attr-name" title={attr.description || ''}>
                                            {attr.required && <span className="required-star">*</span>}
                                            {attr.name}
                                          </span>
                                          <div className="attr-actions">
                                            <span className={`attr-source ${attr.source}`} title={`Źródło: ${attr.source}`}>
                                              {attr.source === 'ai' ? '🤖' : attr.source === 'pim' ? '📋' : '👤'}
                                            </span>
                                            <button 
                                              className="attr-delete-btn" 
                                              onClick={() => {
                                                const newAttrs = allAttributes.filter((_, i) => 
                                                  allAttributes.indexOf(attr) !== allAttributes.indexOf(allAttributes[allAttributes.indexOf(attr)])
                                                );
                                                updateContent(activeProduct, activeMarket, 'amazon_attributes', {
                                                  ...editableContent[activeProduct][activeMarket].amazon_attributes,
                                                  attributes: allAttributes.filter(a => a.name !== attr.name),
                                                  total: allAttributes.length - 1
                                                });
                                              }}
                                              title="Usuń atrybut"
                                            >×</button>
                                          </div>
                                        </div>
                                        {attr.type === 'enum' && attr.values?.length > 0 ? (
                                          <select
                                            className="attr-input"
                                            value={attr.value || ''}
                                            onChange={(e) => {
                                              const newAttrs = [...allAttributes];
                                              const attrIdx = newAttrs.findIndex(a => a.name === attr.name);
                                              if (attrIdx >= 0) {
                                                newAttrs[attrIdx] = { ...newAttrs[attrIdx], value: e.target.value, source: 'user' };
                                                updateContent(activeProduct, activeMarket, 'amazon_attributes', {
                                                  ...editableContent[activeProduct][activeMarket].amazon_attributes,
                                                  attributes: newAttrs
                                                });
                                              }
                                            }}
                                          >
                                            <option value="">-- wybierz --</option>
                                            {attr.values.map(v => <option key={v} value={v}>{v}</option>)}
                                          </select>
                                        ) : attr.name.startsWith('bullet_point') || attr.name === 'product_description' ? (
                                          <textarea
                                            className="attr-input attr-textarea"
                                            value={attr.value || ''}
                                            rows={attr.name === 'product_description' ? 4 : 2}
                                            onChange={(e) => {
                                              const newAttrs = [...allAttributes];
                                              const attrIdx = newAttrs.findIndex(a => a.name === attr.name);
                                              if (attrIdx >= 0) {
                                                newAttrs[attrIdx] = { ...newAttrs[attrIdx], value: e.target.value, source: 'user' };
                                                updateContent(activeProduct, activeMarket, 'amazon_attributes', {
                                                  ...editableContent[activeProduct][activeMarket].amazon_attributes,
                                                  attributes: newAttrs
                                                });
                                              }
                                            }}
                                            placeholder={attr.description || ''}
                                          />
                                        ) : (
                                          <input
                                            type={attr.type === 'integer' ? 'number' : attr.type === 'decimal' ? 'number' : 'text'}
                                            step={attr.type === 'decimal' ? '0.01' : undefined}
                                            className="attr-input"
                                            value={attr.value || ''}
                                            onChange={(e) => {
                                              const newAttrs = [...allAttributes];
                                              const attrIdx = newAttrs.findIndex(a => a.name === attr.name);
                                              if (attrIdx >= 0) {
                                                newAttrs[attrIdx] = { ...newAttrs[attrIdx], value: e.target.value, source: 'user' };
                                                updateContent(activeProduct, activeMarket, 'amazon_attributes', {
                                                  ...editableContent[activeProduct][activeMarket].amazon_attributes,
                                                  attributes: newAttrs
                                                });
                                              }
                                            }}
                                            placeholder={attr.description || attr.name}
                                          />
                                        )}
                                      </div>
                                    );
                                  };
                                  
                                  return (
                                    <>
                                      {/* 1. IDENTIFIERS */}
                                      <div className="attributes-section">
                                        <h5>🔑 Identyfikatory produktu</h5>
                                        <div className="attributes-grid editable">
                                          {identifiers.map((attr, idx) => renderEditableAttr(attr, idx, 'id'))}
                                        </div>
                                      </div>
                                      
                                      {/* 2. LISTING DATA */}
                                      <div className="attributes-section listing-attributes">
                                        <h5>💰 Dane oferty</h5>
                                        <div className="listing-fields">
                                          <div className="listing-field">
                                            <label>Cena</label>
                                            <input type="number" step="0.01" className="attr-input"
                                              value={editableContent[activeProduct]?.[activeMarket]?.listing_price || ''}
                                              onChange={(e) => updateContent(activeProduct, activeMarket, 'listing_price', e.target.value)}
                                              placeholder="0.00" />
                                            <span className="currency">{activeMarket === 'PL' ? 'PLN' : 'EUR'}</span>
                                          </div>
                                          <div className="listing-field">
                                            <label>Ilość</label>
                                            <input type="number" className="attr-input"
                                              value={editableContent[activeProduct]?.[activeMarket]?.listing_quantity || 0}
                                              onChange={(e) => updateContent(activeProduct, activeMarket, 'listing_quantity', parseInt(e.target.value) || 0)} />
                                          </div>
                                          <div className="listing-field">
                                            <label>Stan</label>
                                            <select className="attr-input"
                                              value={editableContent[activeProduct]?.[activeMarket]?.condition_type || 'New'}
                                              onChange={(e) => updateContent(activeProduct, activeMarket, 'condition_type', e.target.value)}>
                                              <option value="New">Nowy</option>
                                              <option value="Refurbished">Odnowiony</option>
                                              <option value="UsedLikeNew">Używany - jak nowy</option>
                                            </select>
                                          </div>
                                          <div className="listing-field">
                                            <label>Operacja</label>
                                            <select className="attr-input"
                                              value={editableContent[activeProduct]?.[activeMarket]?.update_delete || 'Update'}
                                              onChange={(e) => updateContent(activeProduct, activeMarket, 'update_delete', e.target.value)}>
                                              <option value="Update">Update</option>
                                              <option value="PartialUpdate">Partial</option>
                                              <option value="Delete">Delete</option>
                                            </select>
                                          </div>
                                        </div>
                                      </div>
                                      
                                      {/* 3. BULLET POINTS - ALL EDITABLE */}
                                      <div className="attributes-section">
                                        <h5>📝 Bullet Points ({bulletPoints.length})</h5>
                                        <div className="attributes-list vertical">
                                          {bulletPoints.sort((a, b) => a.name.localeCompare(b.name)).map((attr, idx) => renderEditableAttr(attr, idx, 'bp'))}
                                        </div>
                                      </div>
                                      
                                      {/* 4. PHYSICAL ATTRIBUTES */}
                                      <div className="attributes-section">
                                        <h5>📐 Atrybuty fizyczne</h5>
                                        <div className="attributes-grid editable">
                                          {physicalAttrs.map((attr, idx) => renderEditableAttr(attr, idx, 'phys'))}
                                        </div>
                                      </div>
                                      
                                      {/* 5. SPECIAL FEATURES */}
                                      {specialFeatures.length > 0 && (
                                        <div className="attributes-section">
                                          <h5>⭐ Special Features ({specialFeatures.length})</h5>
                                          <div className="attributes-list vertical">
                                            {specialFeatures.sort((a, b) => a.name.localeCompare(b.name)).map((attr, idx) => renderEditableAttr(attr, idx, 'sf'))}
                                          </div>
                                        </div>
                                      )}
                                      
                                      {/* 6. OTHER ATTRIBUTES */}
                                      {otherAttrs.length > 0 && (
                                        <details className="attributes-section collapsible">
                                          <summary>📋 Pozostałe atrybuty ({otherAttrs.length})</summary>
                                          <div className="attributes-grid editable">
                                            {otherAttrs.map((attr, idx) => renderEditableAttr(attr, idx, 'other'))}
                                          </div>
                                        </details>
                                      )}
                                      
                                      {/* ADD NEW ATTRIBUTE */}
                                      <div className="add-attribute-section">
                                        <button className="add-attr-btn" onClick={() => {
                                          const newAttrName = prompt('Nazwa nowego atrybutu:');
                                          if (newAttrName && newAttrName.trim()) {
                                            const newAttr = {
                                              name: newAttrName.trim(),
                                              value: '',
                                              type: 'string',
                                              source: 'user',
                                              required: false
                                            };
                                            updateContent(activeProduct, activeMarket, 'amazon_attributes', {
                                              ...editableContent[activeProduct][activeMarket].amazon_attributes,
                                              attributes: [...allAttributes, newAttr],
                                              total: allAttributes.length + 1
                                            });
                                          }
                                        }}>
                                          ➕ Dodaj atrybut
                                        </button>
                                      </div>
                                    </>
                                  );
                                })()}
                              </details>
                            )}

                            {/* Market Actions */}
                            <div className="market-actions">
                              <div className="regenerate-dropdown-container">
                                <button 
                                  className={`action-btn regenerate ${regenerating ? 'loading' : ''}`}
                                  onClick={() => setShowRegenerateMenu(!showRegenerateMenu)}
                                  disabled={regenerating}
                                >
                                  <RefreshCw size={16} className={regenerating ? 'spin' : ''} />
                                  {regenerating ? 'Regeneruję...' : 'Regeneruj AI ▾'}
                                </button>
                                
                                {showRegenerateMenu && (
                                  <div className="regenerate-menu">
                                    <div className="regenerate-menu-header">
                                      Wybierz pola do regeneracji:
                                    </div>
                                    <label className="regenerate-option">
                                      <input 
                                        type="checkbox" 
                                        checked={regenerateFields.title}
                                        onChange={(e) => setRegenerateFields(prev => ({...prev, title: e.target.checked}))}
                                      />
                                      📝 Tytuł (item_name)
                                    </label>
                                    <label className="regenerate-option">
                                      <input 
                                        type="checkbox" 
                                        checked={regenerateFields.bullets}
                                        onChange={(e) => setRegenerateFields(prev => ({...prev, bullets: e.target.checked}))}
                                      />
                                      📋 Bullet Points
                                    </label>
                                    <label className="regenerate-option">
                                      <input 
                                        type="checkbox" 
                                        checked={regenerateFields.description}
                                        onChange={(e) => setRegenerateFields(prev => ({...prev, description: e.target.checked}))}
                                      />
                                      📄 Opis produktu
                                    </label>
                                    <label className="regenerate-option">
                                      <input 
                                        type="checkbox" 
                                        checked={regenerateFields.keywords}
                                        onChange={(e) => setRegenerateFields(prev => ({...prev, keywords: e.target.checked}))}
                                      />
                                      🔑 Keywords
                                    </label>
                                    <div className="regenerate-menu-actions">
                                      <button 
                                        className="regenerate-select-all"
                                        onClick={() => setRegenerateFields({title: true, bullets: true, description: true, keywords: true})}
                                      >
                                        Zaznacz wszystko
                                      </button>
                                      <button 
                                        className="regenerate-confirm"
                                        onClick={() => regenerateContent(activeProduct, activeMarket, regenerateFields)}
                                        disabled={!Object.values(regenerateFields).some(v => v)}
                                      >
                                        🔄 Regeneruj
                                      </button>
                                    </div>
                                  </div>
                                )}
                              </div>
                              <button 
                                className={`action-btn ${marketStatus[`${activeProduct}_${activeMarket}`] === 'approved' ? 'approved' : 'approve'}`}
                                onClick={() => setMarketApproval(activeProduct, activeMarket, 'approved')}
                              >
                                <CheckCircle2 size={16} />
                                {marketStatus[`${activeProduct}_${activeMarket}`] === 'approved' ? 'Zaakceptowane ✓' : 'Akceptuj'}
                              </button>
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  ) : (
                    <div className="no-product-selected">
                      <Package size={64} />
                      <h3>Wybierz produkt z listy</h3>
                      <p>Kliknij na produkt po lewej stronie, aby zobaczyć podgląd i edytować treści.</p>
                    </div>
                  )}
                </div>
              </div>

              {/* Step Actions */}
              <div className="step-actions">
                <button className="action-btn secondary" onClick={() => setStep(3)}>
                  <ArrowLeft size={18} />
                  Wróć do konfiguracji
                </button>
                
                {/* Family Builder Button */}
                <button 
                  className="action-btn family-builder-btn"
                  onClick={() => setShowFamilyBuilder(true)}
                  title="Utwórz rodziny produktów (Parent-Child)"
                >
                  <GitBranch size={18} />
                  Utwórz rodziny ({families.length})
                </button>
                
                <div className="approval-status">
                  {allMarketsApproved() ? (
                    <span className="all-approved">✅ Wszystkie rynki zaakceptowane</span>
                  ) : (
                    <span className="pending-approval">
                      ⏳ Zaakceptuj wszystkie rynki aby kontynuować
                    </span>
                  )}
                </div>
                <button 
                  className="action-btn primary" 
                  onClick={() => {
                    // Save edited content and proceed to export
                    setStep(5);
                  }}
                  disabled={!allMarketsApproved()}
                >
                  <Download size={18} />
                  Generuj eksport
                </button>
              </div>
              
              {/* Product Family Manager Modal */}
              {showFamilyBuilder && (
                <ProductFamilyManager
                  products={Object.entries(editableContent).map(([kNumber, data]) => ({
                    k_number: kNumber,
                    sku: data.sku || kNumber,
                    title: data.DE?.title || data.title || 'Produkt bez nazwy',
                    color: data.attributes?.color,
                    size: data.attributes?.size,
                    capacity: data.attributes?.capacity,
                    ean: data.ean || '',
                    images: data.images || []
                  }))}
                  sessionId={sessionId}
                  productType={selectedProductType}
                  marketplace={selectedMarkets[0] || 'DE'}
                  onFamilyCreated={(newFamily) => {
                    setFamilies(prev => [...prev, newFamily]);
                    addAiLog('success', `✅ Utworzono rodzinę: ${newFamily.family_name}`, {
                      parent_sku: newFamily.sku,
                      children: newFamily.children?.length || 0
                    });
                  }}
                  onClose={() => setShowFamilyBuilder(false)}
                />
              )}
            </motion.div>
          )}

          {/* Step 5: Complete */}
          {step === 5 && (
            <motion.div
              key="step5"
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              className="step-content"
            >
              <div className="complete-panel">
                <div className="complete-icon">
                  <CheckCircle2 size={64} />
                </div>
                <h2>Eksport zakończony!</h2>
                <p>Twoje pliki zostały pomyślnie wygenerowane i są gotowe do pobrania.</p>
                
                <div className="complete-stats">
                  <div className="stat">
                    <span className="stat-value">{parsedData?.totalRows || 0}</span>
                    <span className="stat-label">Produktów</span>
                  </div>
                  <div className="stat">
                    <span className="stat-value">{families.length}</span>
                    <span className="stat-label">Rodzin</span>
                  </div>
                </div>

                <div className="complete-actions">
                  <button className="action-btn primary">
                    <Download size={18} />
                    Pobierz pliki
                  </button>
                  <button className="action-btn secondary" onClick={() => {
                    setStep(1);
                    setParsedData(null);
                    setFamilies([]);
                  }}>
                    <RefreshCw size={18} />
                    Nowy import
                  </button>
                </div>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </main>
    </div>
  );
}

export default App;
