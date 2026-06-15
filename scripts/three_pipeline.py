"""
======================================================
E-Commerce ETL Pipeline — 3D Visualizer Generator
======================================================
Generates an interactive, WebGL-based 3D scene using Three.js.
Renders the pipeline architecture as 3D nodes with animated
data flows, camera controls, and interactive raycasting.
======================================================
"""

import json

def get_pipeline_html(state="Idle"):
    """
    Returns a self-contained HTML page containing a Three.js scene
    visualizing the ETL pipeline based on the current execution state.
    
    States: "Idle", "Running", "Completed", "Error"
    """
    
    # Node metadata to pass to the JS Raycaster
    node_metadata = {
        "sources": {
            "title": "📥 Raw Data Sources",
            "desc": "Initial ingest layer containing files: orders.csv (100k+ rows), customers.csv (5k rows), and products.csv (500 rows). Generated dynamically via Faker.",
            "tech": "Faker, Python, Pandas CSV Ingestion",
            "metrics": "100,000+ Raw Rows | 3 Source CSVs"
        },
        "extract": {
            "title": "⚙️ Extract Stage",
            "desc": "Scans raw directories, extracts schema structures, and loads files into staging DataFrames. Validates file integrity before processing.",
            "tech": "Pandas, OS Ingestion Scripts",
            "metrics": "Duration: ~0.5s | Memory: 15.4 MB"
        },
        "transform": {
            "title": "🔄 Transform Stage",
            "desc": "Converts staging data into a clean Star-Schema. Calculates revenue, maps dates, generates unique surrogate keys, and builds relationships.",
            "tech": "Pandas, NumPy, SQLAlchemy",
            "metrics": "4 Warehouse Tables Generated"
        },
        "validate": {
            "title": "✅ Validate Stage",
            "desc": "Runs 16 data quality validation checks. Validates primary key uniqueness, foreign key constraints, null value limits, and value ranges.",
            "tech": "Great Expectations / Custom Validate rules",
            "metrics": "16 / 16 Checks Passed | 100% Quality Score"
        },
        "load": {
            "title": "📤 Load Stage",
            "desc": "Persists cleaned data into the SQLite Data Warehouse or pushes directly to Google BigQuery. Supports incremental inserts or full rewrites.",
            "tech": "SQLite3, Google Cloud BigQuery client",
            "metrics": "Target: SQlite (ecommerce_dwh.db)"
        },
        "dwh": {
            "title": "🗄️ Star-Schema DWH",
            "desc": "The central analytics warehouse database. Composed of fact_orders linked to dim_customers, dim_products, and dim_date.",
            "tech": "SQL, SQLite Database engine",
            "metrics": "96,000+ Fact Rows | 3 Dimensions"
        },
        "bi": {
            "title": "📊 BI & Analytics Layer",
            "desc": "Computes business metrics, cohort retentions, AOV, and customer lifetime values. Prepares flat exports optimized for Power BI Desktop.",
            "tech": "Plotly Express, Streamlit, Power BI CSV",
            "metrics": "8 Analytical Queries | DAX Measures"
        }
    }
    
    node_metadata_json = json.dumps(node_metadata)

    html_code = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>3D Pipeline Visualizer</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <!-- Include Three.js, OrbitControls, and GSAP -->
    <script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/three@0.128.0/examples/js/controls/OrbitControls.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/gsap/3.12.2/gsap.min.js"></script>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
    <style>
        body {{
            margin: 0;
            overflow: hidden;
            font-family: 'Inter', sans-serif;
            background-color: #020617;
            color: #f8fafc;
        }}
        canvas {{
            display: block;
            width: 100vw;
            height: 100vh;
        }}
        .glass {{
            background: rgba(15, 23, 42, 0.45);
            backdrop-filter: blur(12px);
            -webkit-backdrop-filter: blur(12px);
            border: 1px solid rgba(99, 102, 241, 0.2);
            border-radius: 16px;
        }}
        /* Hide scrollbars */
        ::-webkit-scrollbar {{
            display: none;
        }}
    </style>
</head>
<body>

    <!-- UI Overlay -->
    <div class="absolute inset-0 pointer-events-none flex flex-col justify-between p-4 z-10">
        
        <!-- Header -->
        <div class="flex justify-between items-center w-full pointer-events-auto">
            <div class="glass px-4 py-2 flex items-center gap-3">
                <span class="relative flex h-3.5 w-3.5">
                    <span class="animate-ping absolute inline-flex h-full w-full rounded-full opacity-75 opacity-100" id="status-ping"></span>
                    <span class="relative inline-flex rounded-full h-3.5 w-3.5" id="status-dot"></span>
                </span>
                <div>
                    <div class="text-[10px] text-slate-400 font-semibold uppercase tracking-wider">System State</div>
                    <div class="text-sm font-bold text-slate-100" id="status-text">{state}</div>
                </div>
            </div>
            
            <div class="glass px-3 py-1.5 text-[11px] text-slate-300 font-medium">
                🖱️ Left-Click + Drag: Rotate | Right-Click: Pan | Scroll: Zoom
            </div>
        </div>

        <!-- Detail Panel -->
        <div class="flex justify-between items-end w-full pointer-events-auto gap-4 mt-auto">
            <!-- Left Info Panel (Dynamic) -->
            <div id="detail-panel" class="glass p-5 w-96 transition-all duration-300 transform translate-y-0 opacity-100 max-h-[220px] overflow-y-auto">
                <div class="text-xs text-indigo-400 font-bold uppercase tracking-widest mb-1" id="detail-category">3D Interactive DWH Architecture</div>
                <h3 class="text-lg font-extrabold text-white mb-2" id="detail-title">Pipeline Orchestration</h3>
                <p class="text-xs text-slate-300 leading-relaxed mb-3" id="detail-desc">
                    Click on any node in the 3D pipeline flow to inspect schema metadata, data quality check scores, and metrics.
                </p>
                <div class="flex justify-between items-center text-[10px] border-t border-indigo-500/20 pt-2.5">
                    <div>
                        <span class="text-slate-400">Tech Stack:</span>
                        <span class="text-indigo-300 font-semibold ml-1" id="detail-tech">WebGL, Three.js</span>
                    </div>
                    <div class="text-emerald-400 font-bold" id="detail-metrics">Interactive Scene</div>
                </div>
            </div>
            
            <!-- Quick Metrics Overview -->
            <div class="glass p-4 w-72 flex flex-col gap-2">
                <div class="text-[10px] text-slate-400 font-semibold uppercase tracking-wider">Flow Control Sim</div>
                <div class="flex gap-2">
                    <button onclick="setSystemState('Idle')" class="flex-1 text-[10px] font-bold py-1 px-2 rounded bg-slate-800 hover:bg-slate-700 transition">Idle</button>
                    <button onclick="setSystemState('Running')" class="flex-1 text-[10px] font-bold py-1 px-2 rounded bg-indigo-600 hover:bg-indigo-500 text-white transition">Run</button>
                    <button onclick="setSystemState('Completed')" class="flex-1 text-[10px] font-bold py-1 px-2 rounded bg-emerald-600 hover:bg-emerald-500 text-white transition">Success</button>
                </div>
            </div>
        </div>
    </div>

    <!-- 3D Viewport container -->
    <div id="canvas-container"></div>

    <script>
        const metadata = {node_metadata_json};
        let systemState = "{state}";

        // Colors
        const COLORS = {{
            idle: 0x6366f1, // indigo
            running: 0xf59e0b, // amber
            completed: 0x10b981, // emerald
            error: 0xef4444, // red
            neonBlue: 0x06b6d4, // cyan
            dwhGold: 0xeab308, // yellow
            biPink: 0xec4899 // pink
        }};

        // 1. Setup Scene, Camera, Renderer
        const container = document.getElementById('canvas-container');
        const scene = new THREE.Scene();
        scene.fog = new THREE.FogExp2(0x020617, 0.015);

        const camera = new THREE.PerspectiveCamera(45, window.innerWidth / window.innerHeight, 0.1, 1000);
        camera.position.set(0, 10, 42);

        const renderer = new THREE.WebGLRenderer({{ antialias: true, alpha: false }});
        renderer.setSize(window.innerWidth, window.innerHeight);
        renderer.setPixelRatio(window.devicePixelRatio);
        renderer.shadowMap.enabled = true;
        renderer.setClearColor(0x020617, 1);
        container.appendChild(renderer.domElement);

        // Orbit Controls
        const controls = new THREE.OrbitControls(camera, renderer.domElement);
        controls.enableDamping = true;
        controls.dampingFactor = 0.05;
        controls.maxPolarAngle = Math.PI / 2 + 0.1; // Don't go below ground level
        controls.minDistance = 15;
        controls.maxDistance = 80;

        // 2. Lights
        const ambientLight = new THREE.AmbientLight(0xffffff, 0.4);
        scene.add(ambientLight);

        const dirLight1 = new THREE.DirectionalLight(0xffffff, 0.8);
        dirLight1.position.set(10, 30, 15);
        scene.add(dirLight1);

        const dirLight2 = new THREE.DirectionalLight(0x6366f1, 0.5);
        dirLight2.position.set(-10, -10, -10);
        scene.add(dirLight2);

        // Point light that follows cursor or floats
        const pointLight = new THREE.PointLight(0x6366f1, 2, 40);
        pointLight.position.set(0, 5, 0);
        scene.add(pointLight);

        // 3. Grid / Ground
        const gridHelper = new THREE.GridHelper(200, 50, 0x1e293b, 0x0f172a);
        gridHelper.position.y = -5;
        scene.add(gridHelper);

        // 4. Create Nodes
        const nodes = [];
        const clickableObjects = [];

        function createNode(id, name, type, x, y, z, color, size = 1.5) {{
            let geometry;
            if (type === 'cube') {{
                geometry = new THREE.BoxGeometry(size * 1.3, size * 1.3, size * 1.3);
            }} else if (type === 'cylinder') {{
                geometry = new THREE.CylinderGeometry(size, size, size * 1.2, 16);
            }} else if (type === 'torus') {{
                geometry = new THREE.TorusGeometry(size * 0.8, size * 0.3, 12, 24);
            }} else if (type === 'double-cone') {{
                geometry = new THREE.OctahedronGeometry(size);
            }} else {{
                geometry = new THREE.SphereGeometry(size, 32, 32);
            }}

            const material = new THREE.MeshPhongMaterial({{
                color: color,
                emissive: color,
                emissiveIntensity: 0.25,
                shininess: 100,
                transparent: true,
                opacity: 0.9
            }});

            const mesh = new THREE.Mesh(geometry, material);
            mesh.position.set(x, y, z);
            mesh.castShadow = true;
            mesh.receiveShadow = true;
            mesh.userData = {{ id: id, name: name, originalColor: color, type: type }};
            scene.add(mesh);
            nodes.push(mesh);
            clickableObjects.push(mesh);

            // Add wireframe outer ring/shield for validation or special nodes
            if (id === 'validate') {{
                const ringGeo = new THREE.RingGeometry(size * 1.4, size * 1.5, 32);
                const ringMat = new THREE.MeshBasicMaterial({{ color: color, side: THREE.DoubleSide, transparent: true, opacity: 0.5 }});
                const ring = new THREE.Mesh(ringGeo, ringMat);
                ring.rotation.x = Math.PI / 2;
                mesh.add(ring);
                mesh.userData.outerRing = ring;
            }}

            // Add text tag floating above
            // (Using small spheres for labels in 3D is heavy, we'll draw simple 3D geometry tag indicators)
            const tagGeo = new THREE.BoxGeometry(0.2, 0.4, 0.2);
            const tagMat = new THREE.MeshBasicMaterial({{ color: 0xffffff }});
            const tag = new THREE.Mesh(tagGeo, tagMat);
            tag.position.y = size + 0.8;
            mesh.add(tag);

            return mesh;
        }}

        // Nodes Configuration
        // Layout horizontally along X axis
        const sourceNode = createNode('sources', 'Raw Sources', 'cube', -20, 0, 0, COLORS.neonBlue, 1.4);
        const extractNode = createNode('extract', 'Extract Stage', 'torus', -13, 0, 0, COLORS.idle, 1.2);
        const transformNode = createNode('transform', 'Transform', 'double-cone', -6, 0, 0, COLORS.idle, 1.3);
        const validateNode = createNode('validate', 'Validate', 'sphere', 1, 0, 0, COLORS.idle, 1.2);
        const loadNode = createNode('load', 'Load', 'cylinder', 8, 0, 0, COLORS.idle, 1.2);
        
        // Star DWH Cluster (X=15, Y=0, Z=0)
        const dwhCenter = createNode('dwh', 'Star Schema DWH', 'cube', 15, 0, 0, COLORS.dwhGold, 1.6);
        // Dim satellites orbiting
        const dimCust = createNode('dwh_cust', 'dim_customers', 'sphere', 15, 3.5, -1, COLORS.dwhGold * 0.8, 0.6);
        const dimProd = createNode('dwh_prod', 'dim_products', 'sphere', 13.5, -2, 2.5, COLORS.dwhGold * 0.8, 0.6);
        const dimDate = createNode('dwh_date', 'dim_date', 'sphere', 16.5, -2, -2.5, COLORS.dwhGold * 0.8, 0.6);

        const biNode = createNode('bi', 'BI & Analytics', 'cube', 23, 0, 0, COLORS.biPink, 1.4);

        // 5. Connective Cables (Bezier Curves)
        const connections = [];
        
        function drawConnection(p1, p2, color) {{
            const start = p1.position;
            const end = p2.position;
            
            // Generate curve
            const controlPoint = new THREE.Vector3((start.x + end.x)/2, (start.y + end.y)/2 + 2.5, (start.z + end.z)/2);
            const curve = new THREE.QuadraticBezierCurve3(start, controlPoint, end);
            
            const points = curve.getPoints(30);
            const geometry = new THREE.BufferGeometry().setFromPoints(points);
            const material = new THREE.LineBasicMaterial({{ color: color, linewidth: 2, transparent: true, opacity: 0.6 }});
            const line = new THREE.Line(geometry, material);
            scene.add(line);
            
            connections.push({{ curve: curve, points: points, line: line }});
        }}

        drawConnection(sourceNode, extractNode, 0x4f46e5);
        drawConnection(extractNode, transformNode, 0x4f46e5);
        drawConnection(transformNode, validateNode, 0x4f46e5);
        drawConnection(validateNode, loadNode, 0x4f46e5);
        drawConnection(loadNode, dwhCenter, 0xeab308);
        drawConnection(dwhCenter, biNode, 0xec4899);

        // Connect central DWH to dimensions directly
        drawConnection(dwhCenter, dimCust, 0xeab308);
        drawConnection(dwhCenter, dimProd, 0xeab308);
        drawConnection(dwhCenter, dimDate, 0xeab308);

        // 6. Particles Flow In Pipeline
        const particles = [];
        const particleGeometry = new THREE.SphereGeometry(0.18, 8, 8);

        function spawnParticle(connectionIndex, speedScale = 1) {{
            const conn = connections[connectionIndex];
            if (!conn) return;

            const material = new THREE.MeshBasicMaterial({{
                color: systemState === 'Running' ? COLORS.running : COLORS.neonBlue,
                transparent: true,
                opacity: 0.9
            }});
            const mesh = new THREE.Mesh(particleGeometry, material);
            scene.add(mesh);

            particles.push({{
                mesh: mesh,
                curve: conn.curve,
                t: 0,
                speed: (0.005 + Math.random() * 0.007) * speedScale,
                connectionIndex: connectionIndex
            }});
        }}

        // 7. Ambient Particle Cloud (Stars/Data background)
        const starGeo = new THREE.BufferGeometry();
        const starCount = 300;
        const starPositions = new Float32Array(starCount * 3);
        for (let i = 0; i < starCount * 3; i += 3) {{
            starPositions[i] = (Math.random() - 0.5) * 120;
            starPositions[i+1] = (Math.random() - 0.2) * 50 + 5;
            starPositions[i+2] = (Math.random() - 0.5) * 120;
        }}
        starGeo.setAttribute('position', new THREE.BufferAttribute(starPositions, 3));
        const starMat = new THREE.PointsMaterial({{
            color: 0x818cf8,
            size: 0.35,
            transparent: true,
            opacity: 0.6
        }});
        const starPoints = new THREE.Points(starGeo, starMat);
        scene.add(starPoints);

        // Update node colors based on pipeline state
        function updateNodeStates() {{
            const dot = document.getElementById('status-dot');
            const ping = document.getElementById('status-ping');
            const text = document.getElementById('status-text');

            text.innerText = systemState;

            let mainColor = COLORS.idle;
            if (systemState === 'Running') {{
                mainColor = COLORS.running;
                dot.className = "relative inline-flex rounded-full h-3.5 w-3.5 bg-amber-500";
                ping.className = "animate-ping absolute inline-flex h-full w-full rounded-full bg-amber-400 opacity-75";
            }} else if (systemState === 'Completed') {{
                mainColor = COLORS.completed;
                dot.className = "relative inline-flex rounded-full h-3.5 w-3.5 bg-emerald-500";
                ping.className = "animate-ping absolute inline-flex h-full w-full rounded-full bg-emerald-400 opacity-75";
            }} else if (systemState === 'Error') {{
                mainColor = COLORS.error;
                dot.className = "relative inline-flex rounded-full h-3.5 w-3.5 bg-red-500";
                ping.className = "animate-ping absolute inline-flex h-full w-full rounded-full bg-red-400 opacity-75";
            }} else {{
                mainColor = COLORS.idle;
                dot.className = "relative inline-flex rounded-full h-3.5 w-3.5 bg-indigo-500";
                ping.className = "animate-ping absolute inline-flex h-full w-full rounded-full bg-indigo-400 opacity-75";
            }}

            // Update materials of core nodes
            const coreNodeIds = ['extract', 'transform', 'validate', 'load'];
            nodes.forEach(node => {{
                if (coreNodeIds.includes(node.userData.id)) {{
                    node.material.color.setHex(mainColor);
                    node.material.emissive.setHex(mainColor);
                    node.userData.originalColor = mainColor;
                }}
            }});
        }}

        function setSystemState(state) {{
            systemState = state;
            updateNodeStates();

            // Trigger animations on state change
            if (state === 'Completed') {{
                // Trigger green ripple animation
                nodes.forEach(n => {{
                    gsap.to(n.scale, {{ x: 1.4, y: 1.4, z: 1.4, duration: 0.3, yoyo: true, repeat: 1, ease: "power2.out" }});
                }});
            }}
        }}

        updateNodeStates();

        // 8. Interaction Logic: Raycasting
        const raycaster = new THREE.Raycaster();
        const mouse = new THREE.Vector2();
        let hoveredObj = null;

        window.addEventListener('mousemove', (event) => {{
            mouse.x = (event.clientX / window.innerWidth) * 2 - 1;
            mouse.y = -(event.clientY / window.innerHeight) * 2 + 1;
        }});

        window.addEventListener('click', () => {{
            raycaster.setFromCamera(mouse, camera);
            const intersects = raycaster.intersectObjects(clickableObjects);

            if (intersects.length > 0) {{
                const clickedNode = intersects[0].object;
                
                // Dim customer/prod/date details map to the parent DWH detail
                let nodeKey = clickedNode.userData.id;
                if (nodeKey.startsWith('dwh_')) {{
                    nodeKey = 'dwh';
                }}
                
                const data = metadata[nodeKey];
                
                if (data) {{
                    // Update UI detail panel
                    document.getElementById('detail-category').innerText = "NODE METADATA";
                    document.getElementById('detail-title').innerText = data.title;
                    document.getElementById('detail-desc').innerText = data.desc;
                    document.getElementById('detail-tech').innerText = data.tech;
                    document.getElementById('detail-metrics').innerText = data.metrics;

                    // Trigger click visual effect (pulse)
                    gsap.to(clickedNode.scale, {{ x: 1.5, y: 1.5, z: 1.5, duration: 0.2, yoyo: true, repeat: 1 }});

                    // Point light flash at node
                    pointLight.position.copy(clickedNode.position);
                    pointLight.color.setHex(clickedNode.userData.originalColor);
                    pointLight.intensity = 4;
                    gsap.to(pointLight, {{ intensity: 2, duration: 0.8 }});

                    // Camera focus on clicked node
                    gsap.to(controls.target, {{
                        x: clickedNode.position.x,
                        y: clickedNode.position.y,
                        z: clickedNode.position.z,
                        duration: 0.8,
                        onUpdate: () => controls.update()
                    }});
                }}
            }}
        }});

        // 9. Animation Loop
        let frameCount = 0;

        function animate() {{
            requestAnimationFrame(animate);
            frameCount++;

            // Rotate nodes
            extractNode.rotation.x += 0.01;
            extractNode.rotation.y += 0.015;

            transformNode.rotation.y += 0.02;
            transformNode.rotation.z += 0.01;

            validateNode.rotation.y += 0.01;
            if (validateNode.userData.outerRing) {{
                validateNode.userData.outerRing.rotation.z -= 0.02;
            }}

            loadNode.rotation.y += 0.005;

            // Slow rotate satellite nodes orbiting DWH Center
            const time = Date.now() * 0.001;
            dimCust.position.x = 15 + Math.cos(time * 0.8) * 3.5;
            dimCust.position.z = Math.sin(time * 0.8) * 3.5;

            dimProd.position.x = 15 + Math.cos(time * 0.6 + 2) * 4;
            dimProd.position.z = Math.sin(time * 0.6 + 2) * 4;

            dimDate.position.x = 15 + Math.cos(time * 0.4 + 4) * 4.5;
            dimDate.position.z = Math.sin(time * 0.4 + 4) * 4.5;

            // Animate line flows (slow dash offset animation)
            connections.forEach(conn => {{
                conn.line.rotation.y += 0.001;
            }});

            // Spawn Particles along pipelines dynamically
            let spawnRate = systemState === 'Running' ? 10 : 80;
            let speedScale = systemState === 'Running' ? 2.5 : 1.0;
            
            if (frameCount % spawnRate === 0) {{
                // Spawn particles on different segments
                const segments = [0, 1, 2, 3, 4, 5];
                segments.forEach(segIndex => spawnParticle(segIndex, speedScale));
            }}

            // Move particles along curves
            for (let i = particles.length - 1; i >= 0; i--) {{
                const p = particles[i];
                p.t += p.speed;

                if (p.t >= 1) {{
                    scene.remove(p.mesh);
                    particles.splice(i, 1);
                }} else {{
                    const pos = p.curve.getPointAt(p.t);
                    p.mesh.position.copy(pos);
                }}
            }}

            // Raycaster Hover glow
            raycaster.setFromCamera(mouse, camera);
            const intersects = raycaster.intersectObjects(clickableObjects);

            if (intersects.length > 0) {{
                const obj = intersects[0].object;
                if (hoveredObj !== obj) {{
                    if (hoveredObj) {{
                        hoveredObj.material.emissiveIntensity = 0.25;
                    }}
                    hoveredObj = obj;
                    obj.material.emissiveIntensity = 0.8;
                }}
            }} else {{
                if (hoveredObj) {{
                    hoveredObj.material.emissiveIntensity = 0.25;
                    hoveredObj = null;
                }}
            }}

            // Slow rotate stars cloud
            starPoints.rotation.y += 0.0003;

            // Slow pointLight floating
            if (systemState !== 'Running' && frameCount % 3 === 0) {{
                pointLight.position.x = Math.sin(time * 0.5) * 15;
                pointLight.position.z = Math.cos(time * 0.3) * 10;
            }}

            controls.update();
            renderer.render(scene, camera);
        }}

        animate();

        // Responsive window resizing
        window.addEventListener('resize', () => {{
            camera.aspect = window.innerWidth / window.innerHeight;
            camera.updateProjectionMatrix();
            renderer.setSize(window.innerWidth, window.innerHeight);
        }});
    </script>
</body>
</html>
    """
    return html_code
